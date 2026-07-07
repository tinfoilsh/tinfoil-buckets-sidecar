"""Tests for boto3's flexible-checksum streaming PUT format.

boto3 1.36+ defaults to uploading PutObject/UploadPart bodies as:

    Transfer-Encoding: chunked
    Content-Encoding: aws-chunked
    X-Amz-Content-Sha256: STREAMING-UNSIGNED-PAYLOAD-TRAILER
    X-Amz-Decoded-Content-Length: <plaintext-length>
    X-Amz-Trailer: x-amz-checksum-crc32
    (no Content-Length)

with the body wrapped in aws-chunked framing and a trailing CRC32 checksum.

test_s3_compat.py / test_multitenant.py / persistent-storage-sim all happen
to point boto3 at `http://localhost:9000`, which trips an endpoint-detection
branch that silently falls back to a header-form PUT *with* Content-Length —
the new streaming format is never exercised.

This file constructs the request by hand with `requests` to make the test
deterministic regardless of boto3's endpoint heuristics.
"""

import base64
import os
import secrets
import uuid
import zlib

import pytest
import requests

ENDPOINT = os.environ.get("ENDPOINT", "http://localhost:9000")
BUCKET = os.environ.get("TEST_BUCKET", "test-bucket-sidecar-1")
SIDECAR_MULTITENANT = os.environ.get("SIDECAR_MULTITENANT", "").lower() == "true"


@pytest.fixture(autouse=True)
def _require_single_tenant_mode():
    """Streaming tests target a single-tenant sidecar. Fail loud if pointed at
    a multitenant server — the request would be missing the per-tenant headers."""
    if SIDECAR_MULTITENANT:
        pytest.fail(
            "test_streaming.py expects a single-tenant sidecar (SIDECAR_MULTITENANT unset).",
            pytrace=False,
        )


# --- aws-chunked encoding helpers -------------------------------------------


def _aws_chunked_body(plaintext: bytes) -> bytes:
    """Wrap plaintext in aws-chunked framing with a trailing CRC32 checksum.

    Format (STREAMING-UNSIGNED-PAYLOAD-TRAILER):

        <hex-length>\\r\\n<bytes>\\r\\n
        0\\r\\n
        x-amz-checksum-crc32:<b64>\\r\\n
        \\r\\n
    """
    crc_bytes = zlib.crc32(plaintext).to_bytes(4, "big")
    crc_b64 = base64.b64encode(crc_bytes).decode("ascii")
    parts = []
    if plaintext:
        parts.append(f"{len(plaintext):x}\r\n".encode("ascii"))
        parts.append(plaintext)
        parts.append(b"\r\n")
    parts.append(b"0\r\n")
    parts.append(f"x-amz-checksum-crc32:{crc_b64}\r\n".encode("ascii"))
    parts.append(b"\r\n")
    return b"".join(parts)


def _aws_chunked_multi(plaintext: bytes, chunk_size: int) -> bytes:
    """Same as above but split plaintext across multiple aws-chunks. Useful for
    exercising the decoder's loop, not just the single-chunk fast path."""
    out = bytearray()
    for i in range(0, len(plaintext), chunk_size):
        slab = plaintext[i : i + chunk_size]
        out += f"{len(slab):x}\r\n".encode("ascii")
        out += slab
        out += b"\r\n"
    crc_bytes = zlib.crc32(plaintext).to_bytes(4, "big")
    crc_b64 = base64.b64encode(crc_bytes).decode("ascii")
    out += b"0\r\n"
    out += f"x-amz-checksum-crc32:{crc_b64}\r\n".encode("ascii")
    out += b"\r\n"
    return bytes(out)


def _stream_in_pieces(body: bytes, pieces: int):
    """Yield `body` in `pieces` roughly-equal pieces so requests sends it via
    HTTP Transfer-Encoding: chunked. Decoupling the HTTP chunks from the
    aws-chunked frames matters: the sidecar's decoder must work no matter how
    the bytes arrive at the socket."""
    if pieces < 1:
        pieces = 1
    step = max(1, len(body) // pieces)
    for i in range(0, len(body), step):
        yield body[i : i + step]


def _streaming_put(
    key: str,
    plaintext: bytes,
    *,
    chunk_size: int | None = None,
    pieces: int = 3,
    content_type: str | None = None,
):
    """Send a PUT in boto3's streaming/chunked format."""
    if chunk_size is None:
        body = _aws_chunked_body(plaintext)
    else:
        body = _aws_chunked_multi(plaintext, chunk_size)

    headers = {
        "Transfer-Encoding": "chunked",
        "Content-Encoding": "aws-chunked",
        "X-Amz-Content-Sha256": "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
        "X-Amz-Decoded-Content-Length": str(len(plaintext)),
        "X-Amz-Trailer": "x-amz-checksum-crc32",
    }
    if content_type:
        headers["Content-Type"] = content_type
    return requests.put(
        f"{ENDPOINT}/{BUCKET}/{key}",
        data=_stream_in_pieces(body, pieces),
        headers=headers,
        timeout=30,
    )


def _get(key: str) -> requests.Response:
    return requests.get(f"{ENDPOINT}/{BUCKET}/{key}", timeout=30)


def _delete(key: str):
    requests.delete(f"{ENDPOINT}/{BUCKET}/{key}", timeout=10)


@pytest.fixture
def key():
    k = f"streaming/{uuid.uuid4()}.bin"
    yield k
    try:
        _delete(k)
    except Exception:
        pass


# --- tests ------------------------------------------------------------------


def test_streaming_put_small_single_chunk(key):
    """4 KiB plaintext in one aws-chunk, delivered in one HTTP chunk."""
    body = secrets.token_bytes(4096)
    r = _streaming_put(key, body, pieces=1)
    assert r.status_code == 200, f"PUT failed: {r.status_code} {r.text}"

    g = _get(key)
    assert g.status_code == 200, f"GET failed: {g.status_code} {g.text}"
    assert g.content == body


def test_streaming_put_split_into_http_chunks(key):
    """Same payload but the HTTP layer splits into 5 chunks — the decoder must
    not assume the whole aws-chunked frame arrives in one read()."""
    body = secrets.token_bytes(64 * 1024)
    r = _streaming_put(key, body, pieces=5)
    assert r.status_code == 200, f"PUT failed: {r.status_code} {r.text}"

    g = _get(key)
    assert g.status_code == 200
    assert g.content == body


def test_streaming_put_multiple_aws_chunks(key):
    """8 KiB plaintext split into 1 KiB aws-chunks — exercises the decode loop."""
    body = secrets.token_bytes(8 * 1024)
    r = _streaming_put(key, body, chunk_size=1024, pieces=3)
    assert r.status_code == 200, f"PUT failed: {r.status_code} {r.text}"

    g = _get(key)
    assert g.status_code == 200
    assert g.content == body


def test_streaming_put_large_round_trip(key):
    """1 MiB streaming PUT. Round-trip proves the decoder handed the right
    plaintext (and the right byte count) to the S3 encryption client."""
    body = secrets.token_bytes(1024 * 1024)
    r = _streaming_put(key, body, chunk_size=64 * 1024, pieces=4)
    assert r.status_code == 200, f"PUT failed: {r.status_code} {r.text}"

    g = _get(key)
    assert g.status_code == 200
    assert g.content == body


def test_streaming_put_preserves_content_type(key):
    """Sidecar must still propagate Content-Type to S3 when on the streaming path."""
    body = b"some text body\n"
    r = _streaming_put(key, body, pieces=1, content_type="text/plain")
    assert r.status_code == 200, f"PUT failed: {r.status_code} {r.text}"

    g = _get(key)
    assert g.status_code == 200
    assert g.content == body
    assert g.headers.get("Content-Type", "").startswith("text/plain")


def test_streaming_uploadpart_round_trip(key):
    """boto3 also streams uploadPart bodies. We create+complete the multipart
    via boto3 (control-plane plumbing) and ship the part itself by hand in the
    streaming aws-chunked format, then verify the assembled object's bytes."""
    import boto3
    from botocore.config import Config

    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id="x",
        aws_secret_access_key="x",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
    )

    body = secrets.token_bytes(5 * 1024 * 1024 + 7)
    mp = s3.create_multipart_upload(Bucket=BUCKET, Key=key)
    upload_id = mp["UploadId"]
    try:
        chunked = _aws_chunked_multi(body, 64 * 1024)
        r = requests.put(
            f"{ENDPOINT}/{BUCKET}/{key}",
            params={"partNumber": "1", "uploadId": upload_id},
            data=_stream_in_pieces(chunked, 4),
            headers={
                "Transfer-Encoding": "chunked",
                "Content-Encoding": "aws-chunked",
                "X-Amz-Content-Sha256": "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
                "X-Amz-Decoded-Content-Length": str(len(body)),
                "X-Amz-Trailer": "x-amz-checksum-crc32",
            },
            timeout=60,
        )
        assert r.status_code == 200, f"uploadPart failed: {r.status_code} {r.text}"
        etag = r.headers["ETag"]

        s3.complete_multipart_upload(
            Bucket=BUCKET,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": [{"ETag": etag, "PartNumber": 1}]},
        )

        g = _get(key)
        assert g.status_code == 200
        assert g.content == body
    finally:
        try:
            s3.abort_multipart_upload(Bucket=BUCKET, Key=key, UploadId=upload_id)
        except Exception:
            pass


def test_streaming_put_missing_decoded_length_rejected(key):
    """aws-chunked without X-Amz-Decoded-Content-Length must be a clean 400 —
    the sidecar has no way to know how big the object actually is otherwise."""
    body = _aws_chunked_body(b"hello")
    headers = {
        "Transfer-Encoding": "chunked",
        "Content-Encoding": "aws-chunked",
        "X-Amz-Content-Sha256": "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
        # Deliberately omitted: X-Amz-Decoded-Content-Length
    }
    r = requests.put(
        f"{ENDPOINT}/{BUCKET}/{key}",
        data=iter([body]),
        headers=headers,
        timeout=10,
    )
    assert r.status_code == 400, f"expected 400, got {r.status_code}: {r.text}"
