"""Multitenant-mode tests.

Requires a sidecar started with MULTITENANT=true. All tests are gated on
SIDECAR_MULTITENANT=true so the file is silently skipped against a single-tenant
sidecar (which the default test suite targets).

  MULTITENANT=true mvn compile exec:java                 # server, one shell
  SIDECAR_MULTITENANT=true ./.venv/bin/pytest -v client/test_multitenant.py
"""

import base64
import os
import secrets
import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("ENDPOINT", "http://localhost:9000")
BUCKET = os.environ.get("TEST_BUCKET", "test-bucket-sidecar-1")
SIDECAR_MULTITENANT = os.environ.get("SIDECAR_MULTITENANT", "").lower() == "true"


@pytest.fixture(autouse=True)
def _require_multitenant_mode():
    """Refuse to run silently against a single-tenant server — would produce
    false negatives (every "tenant isolation" test would pass trivially). Fail
    loudly so accidental runs are obvious."""
    if not SIDECAR_MULTITENANT:
        pytest.fail(
            "test_multitenant.py requires SIDECAR_MULTITENANT=true and a sidecar "
            "started with MULTITENANT=true. Refusing to run against a single-tenant "
            "server (would silently pass).",
            pytrace=False,
        )


# --- Helpers ----------------------------------------------------------------


def _new_aes_key_b64() -> str:
    return base64.b64encode(secrets.token_bytes(32)).decode("ascii")


def _make_client(tenant_id: str | None, enc_key_b64: str | None):
    """boto3 S3 client with optional Tinfoil headers injected on every request.

    Pass tenant_id=None and enc_key_b64=None to omit them (e.g. for negative tests
    that check the sidecar rejects requests missing the headers).
    """
    client = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )

    def add_headers(request, **_):
        if tenant_id is not None:
            request.headers["X-Tinfoil-Tenant-Id"] = tenant_id
        if enc_key_b64 is not None:
            request.headers["X-Tinfoil-Encryption-Key"] = enc_key_b64

    client.meta.events.register_first("before-send.s3.*", add_headers)
    return client


@pytest.fixture
def tenant_a():
    tid = f"tenantA-{uuid.uuid4().hex[:8]}"
    return {"id": tid, "key": _new_aes_key_b64(), "client": None}  # client set below


@pytest.fixture
def tenant_b():
    tid = f"tenantB-{uuid.uuid4().hex[:8]}"
    return {"id": tid, "key": _new_aes_key_b64(), "client": None}


@pytest.fixture
def s3_a(tenant_a):
    tenant_a["client"] = _make_client(tenant_a["id"], tenant_a["key"])
    return tenant_a["client"]


@pytest.fixture
def s3_b(tenant_b):
    tenant_b["client"] = _make_client(tenant_b["id"], tenant_b["key"])
    return tenant_b["client"]


# --- Header validation ------------------------------------------------------


def test_missing_tenant_id_rejected():
    """No headers at all → 400 InvalidArgument referencing the tenant header."""
    bare = _make_client(None, None)
    with pytest.raises(ClientError) as exc:
        bare.put_object(Bucket=BUCKET, Key=f"x/{uuid.uuid4()}", Body=b"x")
    err = exc.value.response
    assert err["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert err["Error"]["Code"] == "InvalidArgument"
    assert "X-Tinfoil-Tenant-Id" in err["Error"]["Message"]


def test_missing_encryption_key_rejected():
    """Tenant header but no key → 400 referencing the key header."""
    no_key = _make_client("tenantX-abc", None)
    with pytest.raises(ClientError) as exc:
        no_key.put_object(Bucket=BUCKET, Key=f"x/{uuid.uuid4()}", Body=b"x")
    err = exc.value.response
    assert err["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert err["Error"]["Code"] == "InvalidArgument"
    assert "X-Tinfoil-Encryption-Key" in err["Error"]["Message"]


def test_invalid_tenant_id_rejected():
    """Slashes / spaces in tenant ID → 400 (regex rejection)."""
    bad = _make_client("not/a/valid/id", _new_aes_key_b64())
    with pytest.raises(ClientError) as exc:
        bad.put_object(Bucket=BUCKET, Key=f"x/{uuid.uuid4()}", Body=b"x")
    err = exc.value.response
    assert err["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert err["Error"]["Code"] == "InvalidArgument"


def test_short_encryption_key_rejected():
    """16-byte key (not AES-256) → 400."""
    short = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
    bad = _make_client(f"tenant-{uuid.uuid4().hex[:8]}", short)
    with pytest.raises(ClientError) as exc:
        bad.put_object(Bucket=BUCKET, Key=f"x/{uuid.uuid4()}", Body=b"x")
    err = exc.value.response
    assert err["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert err["Error"]["Code"] == "InvalidArgument"
    assert "32 bytes" in err["Error"]["Message"]


def test_non_base64_encryption_key_rejected():
    """Non-base64 key → 400."""
    bad = _make_client(f"tenant-{uuid.uuid4().hex[:8]}", "!!!not-base64!!!")
    with pytest.raises(ClientError) as exc:
        bad.put_object(Bucket=BUCKET, Key=f"x/{uuid.uuid4()}", Body=b"x")
    err = exc.value.response
    assert err["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert err["Error"]["Code"] == "InvalidArgument"


# --- Tenant isolation (the core property) -----------------------------------


def test_round_trip_within_tenant(s3_a):
    """Sanity: a tenant can put and get its own object."""
    key = f"mt/{uuid.uuid4()}.bin"
    body = b"hello tenant A"
    try:
        s3_a.put_object(Bucket=BUCKET, Key=key, Body=body)
        resp = s3_a.get_object(Bucket=BUCKET, Key=key)
        assert resp["Body"].read() == body
    finally:
        s3_a.delete_object(Bucket=BUCKET, Key=key)


def test_tenant_b_cannot_see_tenant_a_object(s3_a, s3_b):
    """Tenant B sees 404 on tenant A's key — they live in different prefixes."""
    key = f"mt-iso/{uuid.uuid4()}.bin"
    try:
        s3_a.put_object(Bucket=BUCKET, Key=key, Body=b"private to A")
        with pytest.raises(ClientError) as exc:
            s3_b.get_object(Bucket=BUCKET, Key=key)
        assert exc.value.response["Error"]["Code"] == "NoSuchKey"
    finally:
        s3_a.delete_object(Bucket=BUCKET, Key=key)


def test_list_objects_only_returns_own_tenant(s3_a, s3_b):
    """A list call returns only the calling tenant's keys."""
    prefix = f"mt-list/{uuid.uuid4().hex[:8]}/"
    a_keys = [f"{prefix}a-{i}.txt" for i in range(3)]
    b_keys = [f"{prefix}b-{i}.txt" for i in range(3)]
    try:
        for k in a_keys:
            s3_a.put_object(Bucket=BUCKET, Key=k, Body=b"A")
        for k in b_keys:
            s3_b.put_object(Bucket=BUCKET, Key=k, Body=b"B")

        a_listed = sorted(
            obj["Key"]
            for obj in s3_a.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get(
                "Contents", []
            )
        )
        b_listed = sorted(
            obj["Key"]
            for obj in s3_b.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get(
                "Contents", []
            )
        )
        assert a_listed == sorted(a_keys)
        assert b_listed == sorted(b_keys)
        # Keys round-trip clean — no tenant prefix bleeding into the response.
        for k in a_listed:
            assert not k.startswith("tenantA-")
            assert not k.startswith("tenantB-")
    finally:
        for k in a_keys:
            s3_a.delete_object(Bucket=BUCKET, Key=k)
        for k in b_keys:
            s3_b.delete_object(Bucket=BUCKET, Key=k)


def test_wrong_key_for_own_object_returns_decryption_failed(s3_a, tenant_a):
    """If a caller arrives with the right tenantId but a *rotated* key, decrypt fails cleanly."""
    key = f"mt-wrongkey/{uuid.uuid4()}.bin"
    try:
        s3_a.put_object(Bucket=BUCKET, Key=key, Body=b"sealed with key1")
        # Same tenant, different key — simulates a key rotation where the caller
        # forgot which key encrypted this particular object.
        rotated = _make_client(tenant_a["id"], _new_aes_key_b64())
        with pytest.raises(ClientError) as exc:
            rotated.get_object(Bucket=BUCKET, Key=key)
        err = exc.value.response
        assert err["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert err["Error"]["Code"] == "DecryptionFailed"
    finally:
        # Cleanup uses the right key.
        s3_a.delete_object(Bucket=BUCKET, Key=key)


# --- Multipart isolation ----------------------------------------------------


def test_multipart_session_is_tenant_scoped(s3_a, s3_b):
    """Tenant B can't touch tenant A's uploadId — should look like NoSuchUpload."""
    key = f"mt-mpu/{uuid.uuid4()}.bin"
    mp = s3_a.create_multipart_upload(Bucket=BUCKET, Key=key)
    upload_id = mp["UploadId"]
    try:
        # Tenant B attempting to upload to A's uploadId.
        with pytest.raises(ClientError) as exc:
            s3_b.upload_part(
                Bucket=BUCKET,
                Key=key,
                UploadId=upload_id,
                PartNumber=1,
                Body=b"x" * (5 * 1024 * 1024),
            )
        err = exc.value.response
        assert err["Error"]["Code"] == "NoSuchUpload"
        # Same for abort.
        with pytest.raises(ClientError) as exc:
            s3_b.abort_multipart_upload(Bucket=BUCKET, Key=key, UploadId=upload_id)
        assert exc.value.response["Error"]["Code"] == "NoSuchUpload"
    finally:
        s3_a.abort_multipart_upload(Bucket=BUCKET, Key=key, UploadId=upload_id)


def test_multipart_round_trip_within_tenant(s3_a):
    """End-to-end multipart upload + get under one tenant."""
    key = f"mt-mpu-rt/{uuid.uuid4()}.bin"
    body = b"M" * (5 * 1024 * 1024 + 9)
    mp = s3_a.create_multipart_upload(Bucket=BUCKET, Key=key)
    try:
        part = s3_a.upload_part(
            Bucket=BUCKET,
            Key=key,
            UploadId=mp["UploadId"],
            PartNumber=1,
            Body=body,
        )
        s3_a.complete_multipart_upload(
            Bucket=BUCKET,
            Key=key,
            UploadId=mp["UploadId"],
            MultipartUpload={"Parts": [{"ETag": part["ETag"], "PartNumber": 1}]},
        )
        resp = s3_a.get_object(Bucket=BUCKET, Key=key)
        assert resp["Body"].read() == body
    finally:
        try:
            s3_a.delete_object(Bucket=BUCKET, Key=key)
        except ClientError:
            pass


def test_list_multipart_uploads_only_own_tenant(s3_a, s3_b):
    """A tenant's ListMultipartUploads must not surface another tenant's upload."""
    key_a = f"mt-listmpu-a/{uuid.uuid4()}.bin"
    key_b = f"mt-listmpu-b/{uuid.uuid4()}.bin"
    mp_a = s3_a.create_multipart_upload(Bucket=BUCKET, Key=key_a)
    mp_b = s3_b.create_multipart_upload(Bucket=BUCKET, Key=key_b)
    try:
        ids_a = {
            u["UploadId"]
            for u in s3_a.list_multipart_uploads(Bucket=BUCKET).get("Uploads", [])
        }
        ids_b = {
            u["UploadId"]
            for u in s3_b.list_multipart_uploads(Bucket=BUCKET).get("Uploads", [])
        }
        assert mp_a["UploadId"] in ids_a
        assert mp_b["UploadId"] not in ids_a
        assert mp_b["UploadId"] in ids_b
        assert mp_a["UploadId"] not in ids_b
    finally:
        s3_a.abort_multipart_upload(Bucket=BUCKET, Key=key_a, UploadId=mp_a["UploadId"])
        s3_b.abort_multipart_upload(Bucket=BUCKET, Key=key_b, UploadId=mp_b["UploadId"])
