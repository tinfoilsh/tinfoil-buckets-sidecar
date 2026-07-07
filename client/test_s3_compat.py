"""S3 protocol compatibility tests.

Requires a running sidecar (default http://localhost:9000) and a real upstream
bucket. Defaults to `test-bucket-sidecar-1`; override with TEST_BUCKET.

  ./.venv/bin/pytest -v test_s3_compat.py
"""

import os
import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("ENDPOINT", "http://localhost:9000")
BUCKET = os.environ.get("TEST_BUCKET", "test-bucket-sidecar-1")
SIDECAR_MULTITENANT = os.environ.get("SIDECAR_MULTITENANT", "").lower() == "true"


@pytest.fixture(autouse=True)
def _require_single_tenant_mode():
    """These tests don't send the multitenant headers, so every request would
    400 against a multitenant server. Fail loudly if SIDECAR_MULTITENANT=true
    so the misconfiguration is obvious instead of a wall of InvalidArgument."""
    if SIDECAR_MULTITENANT:
        pytest.fail(
            "test_s3_compat.py is for single-tenant mode. SIDECAR_MULTITENANT=true is "
            "set — use test_multitenant.py instead, or restart the sidecar without "
            "MULTITENANT=true.",
            pytrace=False,
        )


@pytest.fixture(scope="session")
def s3():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


@pytest.fixture
def key(s3):
    k = f"compat/{uuid.uuid4()}.bin"
    yield k
    try:
        s3.delete_object(Bucket=BUCKET, Key=k)
    except ClientError:
        pass


# --- Happy-path response shapes ----------------------------------------------


def test_put_returns_etag_and_200(s3, key):
    resp = s3.put_object(Bucket=BUCKET, Key=key, Body=b"hello")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "ETag" in resp
    # S3 returns ETag as a quoted string
    assert resp["ETag"].startswith('"') and resp["ETag"].endswith('"')


def test_get_returns_body_and_metadata(s3, key):
    body = b"hello world"
    put = s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="text/plain")
    resp = s3.get_object(Bucket=BUCKET, Key=key)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert resp["Body"].read() == body
    assert resp["ContentType"] == "text/plain"
    assert resp["ContentLength"] == len(body)
    assert resp["ETag"] == put["ETag"]


def test_head_returns_plaintext_content_length(s3, key):
    body = b"head me"
    put = s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="text/plain")
    resp = s3.head_object(Bucket=BUCKET, Key=key)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    # Content-Length should reflect plaintext, not ciphertext
    assert resp["ContentLength"] == len(body)
    assert resp["ContentType"] == "text/plain"
    assert resp["ETag"] == put["ETag"]


def test_delete_returns_204(s3, key):
    s3.put_object(Bucket=BUCKET, Key=key, Body=b"x")
    resp = s3.delete_object(Bucket=BUCKET, Key=key)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204


def test_delete_missing_key_is_idempotent(s3):
    # S3 DeleteObject is idempotent — deleting a non-existent key returns 204.
    resp = s3.delete_object(Bucket=BUCKET, Key=f"missing/{uuid.uuid4()}")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204


# --- Error mapping ----------------------------------------------------------


def test_get_missing_raises_no_such_key(s3):
    with pytest.raises(ClientError) as exc:
        s3.get_object(Bucket=BUCKET, Key=f"missing/{uuid.uuid4()}")
    assert exc.value.response["Error"]["Code"] == "NoSuchKey"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_head_missing_returns_404(s3):
    # HEAD on missing key: boto3 raises ClientError with 404 (no body to parse).
    with pytest.raises(ClientError) as exc:
        s3.head_object(Bucket=BUCKET, Key=f"missing/{uuid.uuid4()}")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


# --- Content / key edge cases ------------------------------------------------


def test_empty_body_roundtrip(s3, key):
    s3.put_object(Bucket=BUCKET, Key=key, Body=b"")
    resp = s3.get_object(Bucket=BUCKET, Key=key)
    assert resp["Body"].read() == b""
    assert resp["ContentLength"] == 0


def test_binary_roundtrip(s3, key):
    body = bytes(range(256)) * 16  # 4 KiB of mixed bytes
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)
    resp = s3.get_object(Bucket=BUCKET, Key=key)
    assert resp["Body"].read() == body


def test_utf8_content(s3, key):
    body = "héllo wörld 🔐 中文".encode("utf-8")
    s3.put_object(
        Bucket=BUCKET, Key=key, Body=body, ContentType="text/plain; charset=utf-8"
    )
    resp = s3.get_object(Bucket=BUCKET, Key=key)
    assert resp["Body"].read() == body


def test_key_with_path_separators(s3):
    k = f"deeply/nested/path/{uuid.uuid4()}.txt"
    s3.put_object(Bucket=BUCKET, Key=k, Body=b"nested")
    try:
        resp = s3.get_object(Bucket=BUCKET, Key=k)
        assert resp["Body"].read() == b"nested"
    finally:
        s3.delete_object(Bucket=BUCKET, Key=k)


def test_content_type_passthrough(s3, key):
    s3.put_object(Bucket=BUCKET, Key=key, Body=b"{}", ContentType="application/json")
    resp = s3.get_object(Bucket=BUCKET, Key=key)
    assert resp["ContentType"] == "application/json"


# --- Documented gaps (features not yet implemented) --------------------------


def test_list_objects_v2(s3):
    prefix = f"list-test/{uuid.uuid4()}/"
    keys = [f"{prefix}a.txt", f"{prefix}b.txt", f"{prefix}c.txt"]
    try:
        for k in keys:
            s3.put_object(Bucket=BUCKET, Key=k, Body=b"hello")
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        assert resp["KeyCount"] == 3
        assert resp["IsTruncated"] is False
        assert sorted(obj["Key"] for obj in resp["Contents"]) == sorted(keys)
        assert all(obj["Size"] == len(b"hello") for obj in resp["Contents"])
    finally:
        for k in keys:
            s3.delete_object(Bucket=BUCKET, Key=k)


def test_list_objects_v2_pagination(s3):
    prefix = f"page-test/{uuid.uuid4()}/"
    keys = [f"{prefix}{i:03d}.txt" for i in range(7)]
    try:
        for k in keys:
            s3.put_object(Bucket=BUCKET, Key=k, Body=b"x")
        page1 = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=3)
        assert page1["KeyCount"] == 3
        assert page1["IsTruncated"] is True
        assert "NextContinuationToken" in page1
        page2 = s3.list_objects_v2(
            Bucket=BUCKET,
            Prefix=prefix,
            MaxKeys=3,
            ContinuationToken=page1["NextContinuationToken"],
        )
        assert page2["KeyCount"] == 3
        seen = [obj["Key"] for obj in page1["Contents"]] + [
            obj["Key"] for obj in page2["Contents"]
        ]
        assert sorted(seen) == sorted(keys[:6])
    finally:
        for k in keys:
            s3.delete_object(Bucket=BUCKET, Key=k)


def test_head_bucket(s3):
    resp = s3.head_bucket(Bucket=BUCKET)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_user_metadata_passthrough_head(s3, key):
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=b"x",
        Metadata={"author": "tinfoil", "version": "1"},
    )
    resp = s3.head_object(Bucket=BUCKET, Key=key)
    assert resp["Metadata"]["author"] == "tinfoil"
    assert resp["Metadata"]["version"] == "1"


def test_user_metadata_passthrough_get(s3, key):
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=b"x",
        Metadata={"author": "tinfoil", "purpose": "testing"},
    )
    resp = s3.get_object(Bucket=BUCKET, Key=key)
    assert resp["Metadata"]["author"] == "tinfoil"
    assert resp["Metadata"]["purpose"] == "testing"


def test_delete_objects_batch(s3):
    prefix = f"batch-delete/{uuid.uuid4()}/"
    keys = [f"{prefix}{i}.txt" for i in range(5)]
    for k in keys:
        s3.put_object(Bucket=BUCKET, Key=k, Body=b"x")
    resp = s3.delete_objects(
        Bucket=BUCKET,
        Delete={"Objects": [{"Key": k} for k in keys]},
    )
    deleted = sorted(d["Key"] for d in resp.get("Deleted", []))
    assert deleted == sorted(keys)
    for k in keys:
        with pytest.raises(ClientError) as exc:
            s3.head_object(Bucket=BUCKET, Key=k)
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_get_bucket_location(s3):
    resp = s3.get_bucket_location(Bucket=BUCKET)
    assert "LocationConstraint" in resp


def test_list_multipart_uploads(s3):
    test_key = f"mpu-list/{uuid.uuid4()}.bin"
    mp = s3.create_multipart_upload(Bucket=BUCKET, Key=test_key)
    try:
        resp = s3.list_multipart_uploads(Bucket=BUCKET)
        ids = [u["UploadId"] for u in resp.get("Uploads", [])]
        assert mp["UploadId"] in ids
    finally:
        s3.abort_multipart_upload(Bucket=BUCKET, Key=test_key, UploadId=mp["UploadId"])


def test_list_parts(s3, key):
    mp = s3.create_multipart_upload(Bucket=BUCKET, Key=key)
    try:
        for n in (1, 2, 3):
            s3.upload_part(
                Bucket=BUCKET,
                Key=key,
                UploadId=mp["UploadId"],
                PartNumber=n,
                Body=b"x" * (5 * 1024 * 1024),
            )
        resp = s3.list_parts(Bucket=BUCKET, Key=key, UploadId=mp["UploadId"])
        assert resp["UploadId"] == mp["UploadId"]
        # All three should be visible: parts 1 and 2 flushed upstream, part 3
        # is locally buffered but surfaced so clients see N parts, not N-1.
        part_numbers = sorted(p["PartNumber"] for p in resp["Parts"])
        assert part_numbers == [1, 2, 3]
    finally:
        s3.abort_multipart_upload(Bucket=BUCKET, Key=key, UploadId=mp["UploadId"])


def test_user_metadata_passthrough_multipart(s3, key):
    body = b"y" * (5 * 1024 * 1024 + 3)
    mp = s3.create_multipart_upload(
        Bucket=BUCKET,
        Key=key,
        Metadata={"source": "multipart"},
    )
    part = s3.upload_part(
        Bucket=BUCKET,
        Key=key,
        UploadId=mp["UploadId"],
        PartNumber=1,
        Body=body,
    )
    s3.complete_multipart_upload(
        Bucket=BUCKET,
        Key=key,
        UploadId=mp["UploadId"],
        MultipartUpload={"Parts": [{"ETag": part["ETag"], "PartNumber": 1}]},
    )
    resp = s3.head_object(Bucket=BUCKET, Key=key)
    assert resp["Metadata"]["source"] == "multipart"


def test_multipart_upload_single_part(s3, key):
    body = b"x" * (5 * 1024 * 1024 + 7)  # 5 MiB + 7
    mp = s3.create_multipart_upload(Bucket=BUCKET, Key=key)
    part = s3.upload_part(
        Bucket=BUCKET,
        Key=key,
        UploadId=mp["UploadId"],
        PartNumber=1,
        Body=body,
    )
    s3.complete_multipart_upload(
        Bucket=BUCKET,
        Key=key,
        UploadId=mp["UploadId"],
        MultipartUpload={"Parts": [{"ETag": part["ETag"], "PartNumber": 1}]},
    )
    resp = s3.get_object(Bucket=BUCKET, Key=key)
    assert resp["Body"].read() == body


def test_multipart_upload_three_parts(s3, key):
    p1 = b"A" * (5 * 1024 * 1024)
    p2 = b"B" * (5 * 1024 * 1024)
    p3 = b"C" * (1024)  # last can be small
    mp = s3.create_multipart_upload(Bucket=BUCKET, Key=key)
    e1 = s3.upload_part(
        Bucket=BUCKET, Key=key, UploadId=mp["UploadId"], PartNumber=1, Body=p1
    )["ETag"]
    e2 = s3.upload_part(
        Bucket=BUCKET, Key=key, UploadId=mp["UploadId"], PartNumber=2, Body=p2
    )["ETag"]
    e3 = s3.upload_part(
        Bucket=BUCKET, Key=key, UploadId=mp["UploadId"], PartNumber=3, Body=p3
    )["ETag"]
    s3.complete_multipart_upload(
        Bucket=BUCKET,
        Key=key,
        UploadId=mp["UploadId"],
        MultipartUpload={
            "Parts": [
                {"ETag": e1, "PartNumber": 1},
                {"ETag": e2, "PartNumber": 2},
                {"ETag": e3, "PartNumber": 3},
            ]
        },
    )
    resp = s3.get_object(Bucket=BUCKET, Key=key)
    assert resp["Body"].read() == p1 + p2 + p3


def test_multipart_abort(s3, key):
    mp = s3.create_multipart_upload(Bucket=BUCKET, Key=key)
    s3.upload_part(
        Bucket=BUCKET,
        Key=key,
        UploadId=mp["UploadId"],
        PartNumber=1,
        Body=b"x" * (5 * 1024 * 1024),
    )
    s3.abort_multipart_upload(Bucket=BUCKET, Key=key, UploadId=mp["UploadId"])
    # Object should not exist after abort
    with pytest.raises(ClientError) as exc:
        s3.get_object(Bucket=BUCKET, Key=key)
    assert exc.value.response["Error"]["Code"] == "NoSuchKey"


def test_multipart_out_of_order_rejected(s3, key):
    mp = s3.create_multipart_upload(Bucket=BUCKET, Key=key)
    try:
        with pytest.raises(ClientError) as exc:
            s3.upload_part(
                Bucket=BUCKET,
                Key=key,
                UploadId=mp["UploadId"],
                PartNumber=2,
                Body=b"x" * (5 * 1024 * 1024),
            )
        assert exc.value.response["Error"]["Code"] == "InvalidPart"
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    finally:
        s3.abort_multipart_upload(Bucket=BUCKET, Key=key, UploadId=mp["UploadId"])


@pytest.mark.xfail(reason="Range requests not implemented yet", strict=True)
def test_range_get(s3, key):
    s3.put_object(Bucket=BUCKET, Key=key, Body=b"0123456789")
    resp = s3.get_object(Bucket=BUCKET, Key=key, Range="bytes=2-5")
    assert resp["Body"].read() == b"2345"


# --- Mode-specific tests (opt-in via env vars) -------------------------------
# These tests verify behavior that depends on how the sidecar was started.
# Both are skipped by default. To run them, restart the sidecar in the matching
# mode and set the corresponding test-side env var (see usage below).

SIDECAR_BUFFER_SIZE = os.environ.get("SIDECAR_BUFFER_SIZE")
SIDECAR_DELAYED_AUTH = os.environ.get("SIDECAR_DELAYED_AUTH", "").lower() == "true"


@pytest.mark.skipif(
    SIDECAR_BUFFER_SIZE is None,
    reason=(
        "Set SIDECAR_BUFFER_SIZE=<bytes> (matching the sidecar's BUFFER_SIZE) to run. "
        "Sidecar must be started in default (buffered) mode with that BUFFER_SIZE."
    ),
)
def test_buffer_exceeded_returns_413(s3, key):
    """PUT an object 1 MiB larger than the sidecar's buffer cap, expect 413 EntityTooLarge on GET."""
    assert (
        SIDECAR_BUFFER_SIZE is not None
    )  # skipif-guaranteed; here for the type checker
    cap = int(SIDECAR_BUFFER_SIZE)
    body = b"x" * (cap + 1024 * 1024)
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)
    with pytest.raises(ClientError) as exc:
        s3.get_object(Bucket=BUCKET, Key=key)
    err = exc.value.response
    assert err["ResponseMetadata"]["HTTPStatusCode"] == 413
    assert err["Error"]["Code"] == "EntityTooLarge"
    msg = err["Error"]["Message"]
    assert "BUFFER_SIZE" in msg
    assert "DANGEROUS_DELAYED_AUTH" in msg


@pytest.mark.skipif(
    not SIDECAR_DELAYED_AUTH,
    reason=(
        "Set SIDECAR_DELAYED_AUTH=true to run. Sidecar must be started with "
        "DANGEROUS_DELAYED_AUTH=true."
    ),
)
def test_delayed_auth_streams_with_ok_trailer(s3, key):
    """In delayed-auth mode, verified_get must successfully read the 'ok' trailer."""
    from tinfoil_client import verified_get

    body = b"delayed-auth-payload" * 50_000  # ~1 MiB
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)
    assert verified_get(s3, Bucket=BUCKET, Key=key) == body


@pytest.mark.skipif(
    not SIDECAR_DELAYED_AUTH,
    reason="Set SIDECAR_DELAYED_AUTH=true when sidecar runs with DANGEROUS_DELAYED_AUTH=true.",
)
def test_delayed_auth_verified_iter_yields_clean_plaintext(s3, key):
    """verified_iter must strip the 4-byte marker; concatenated chunks == plaintext."""
    from tinfoil_client import verified_iter

    body = (b"A" * (256 * 1024)) + (b"B" * (256 * 1024))  # 512 KiB
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)
    chunks = list(verified_iter(s3, Bucket=BUCKET, Key=key, chunk_size=64 * 1024))
    assert b"".join(chunks) == body
    assert len(chunks) > 1


@pytest.mark.skipif(
    not SIDECAR_DELAYED_AUTH,
    reason="Set SIDECAR_DELAYED_AUTH=true (with sidecar in DANGEROUS_DELAYED_AUTH=true).",
)
def test_delayed_auth_handles_objects_above_default_buffer(s3, key):
    """Delayed-auth mode must handle objects bigger than the default 64 MiB buffered cap."""
    from tinfoil_client import verified_iter

    body = b"X" * (80 * 1024 * 1024)  # 80 MiB — would fail in default buffered mode
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)
    total = 0
    for chunk in verified_iter(s3, Bucket=BUCKET, Key=key, chunk_size=4 * 1024 * 1024):
        total += len(chunk)
    assert total == len(body)
