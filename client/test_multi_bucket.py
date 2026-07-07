"""Multi-bucket routing tests.

Verifies the sidecar routes to the bucket named in the URL path (the
multi-bucket feature) rather than a single hardcoded bucket. Requires TWO
distinct real buckets that the configured AWS credentials can access:

  TEST_BUCKET    — primary bucket (defaults to test-bucket-sidecar-1)
  TEST_BUCKET_2  — a second bucket (defaults to test-bucket-sidecar-2)

Single-tenant mode only — the test client sends no multitenant headers.

  TEST_BUCKET=alpha TEST_BUCKET_2=beta ./.venv/bin/pytest -v client/test_multi_bucket.py
"""

import os
import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("ENDPOINT", "http://localhost:9000")
BUCKET = os.environ.get("TEST_BUCKET", "test-bucket-sidecar-1")
BUCKET_2 = os.environ.get("TEST_BUCKET_2", "test-bucket-sidecar-2")
SIDECAR_MULTITENANT = os.environ.get("SIDECAR_MULTITENANT", "").lower() == "true"


@pytest.fixture(autouse=True)
def _require_two_buckets_single_tenant():
    """Multi-bucket routing is mode-independent, but this client sends no
    tenant headers — refuse to run against a multitenant sidecar (would 400
    on every request). Fail loudly on missing buckets so the misconfiguration
    is obvious instead of a wall of NoSuchBucket errors."""
    if SIDECAR_MULTITENANT:
        pytest.fail(
            "test_multi_bucket.py is for single-tenant mode. SIDECAR_MULTITENANT=true "
            "is set — restart the sidecar without MULTITENANT=true.",
            pytrace=False,
        )
    if not BUCKET or not BUCKET_2:
        pytest.fail(
            "Set TEST_BUCKET and TEST_BUCKET_2 to two distinct real S3 buckets "
            "your AWS credentials can access.",
            pytrace=False,
        )
    if BUCKET == BUCKET_2:
        pytest.fail("TEST_BUCKET and TEST_BUCKET_2 must be different buckets.", pytrace=False)


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


# --- Single-shot object isolation across buckets ----------------------------


def test_roundtrip_in_each_bucket(s3):
    """Both buckets are independently read/write."""
    ka = f"mb/{uuid.uuid4()}.bin"
    kb = f"mb/{uuid.uuid4()}.bin"
    try:
        s3.put_object(Bucket=BUCKET, Key=ka, Body=b"alpha")
        s3.put_object(Bucket=BUCKET_2, Key=kb, Body=b"beta")
        assert s3.get_object(Bucket=BUCKET, Key=ka)["Body"].read() == b"alpha"
        assert s3.get_object(Bucket=BUCKET_2, Key=kb)["Body"].read() == b"beta"
    finally:
        for b, k in ((BUCKET, ka), (BUCKET_2, kb)):
            try:
                s3.delete_object(Bucket=b, Key=k)
            except ClientError:
                pass


def test_same_key_isolated_across_buckets(s3):
    """The same key in two buckets holds two independent objects."""
    key = f"mb-same/{uuid.uuid4()}.bin"
    try:
        s3.put_object(Bucket=BUCKET, Key=key, Body=b"in A")
        s3.put_object(Bucket=BUCKET_2, Key=key, Body=b"in B")
        assert s3.get_object(Bucket=BUCKET, Key=key)["Body"].read() == b"in A"
        assert s3.get_object(Bucket=BUCKET_2, Key=key)["Body"].read() == b"in B"
    finally:
        for b in (BUCKET, BUCKET_2):
            try:
                s3.delete_object(Bucket=b, Key=key)
            except ClientError:
                pass


def test_cross_bucket_get_is_no_such_key(s3):
    """An object in BUCKET is not visible in BUCKET_2."""
    key = f"mb-cross/{uuid.uuid4()}.bin"
    try:
        s3.put_object(Bucket=BUCKET, Key=key, Body=b"only in A")
        with pytest.raises(ClientError) as exc:
            s3.get_object(Bucket=BUCKET_2, Key=key)
        assert exc.value.response["Error"]["Code"] == "NoSuchKey"
    finally:
        try:
            s3.delete_object(Bucket=BUCKET, Key=key)
        except ClientError:
            pass


def test_list_objects_scoped_per_bucket(s3):
    """ListObjectsV2 on one bucket does not surface the other's objects."""
    prefix = f"mb-list/{uuid.uuid4().hex[:8]}/"
    ka = f"{prefix}a.txt"
    kb = f"{prefix}b.txt"
    try:
        s3.put_object(Bucket=BUCKET, Key=ka, Body=b"A")
        s3.put_object(Bucket=BUCKET_2, Key=kb, Body=b"B")

        a_keys = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", [])]
        b_keys = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET_2, Prefix=prefix).get("Contents", [])]
        assert a_keys == [ka]
        assert b_keys == [kb]
    finally:
        for b, k in ((BUCKET, ka), (BUCKET_2, kb)):
            try:
                s3.delete_object(Bucket=b, Key=k)
            except ClientError:
                pass


def test_head_bucket_both(s3):
    assert s3.head_bucket(Bucket=BUCKET)["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert s3.head_bucket(Bucket=BUCKET_2)["ResponseMetadata"]["HTTPStatusCode"] == 200


# --- Multipart: uploadId is scoped to its (bucket, key) ---------------------
#
# An uploadId is bound to the bucket/key it was created on. Reusing it under a
# different bucket or key must look like NoSuchUpload — otherwise a confused
# client could silently land parts on the wrong object.


def test_multipart_rejected_on_wrong_bucket(s3):
    """uploadPart / listParts / complete / abort on BUCKET_2 with an uploadId
    created on BUCKET must all return NoSuchUpload."""
    key = f"mb-mpu-xbucket/{uuid.uuid4()}.bin"
    mp = s3.create_multipart_upload(Bucket=BUCKET, Key=key)
    upload_id = mp["UploadId"]
    try:
        for op in ("upload_part", "list_parts", "complete", "abort"):
            with pytest.raises(ClientError) as exc:
                if op == "upload_part":
                    s3.upload_part(
                        Bucket=BUCKET_2, Key=key, UploadId=upload_id,
                        PartNumber=1, Body=b"x" * (5 * 1024 * 1024),
                    )
                elif op == "list_parts":
                    s3.list_parts(Bucket=BUCKET_2, Key=key, UploadId=upload_id)
                elif op == "complete":
                    s3.complete_multipart_upload(
                        Bucket=BUCKET_2, Key=key, UploadId=upload_id,
                        MultipartUpload={"Parts": []},
                    )
                elif op == "abort":
                    s3.abort_multipart_upload(Bucket=BUCKET_2, Key=key, UploadId=upload_id)
            assert exc.value.response["Error"]["Code"] == "NoSuchUpload", op
    finally:
        # Cleanup on the original bucket — the uploadId only exists there.
        try:
            s3.abort_multipart_upload(Bucket=BUCKET, Key=key, UploadId=upload_id)
        except ClientError:
            pass


def test_multipart_rejected_on_wrong_key(s3):
    """Same bucket, different key — uploadId must still look like NoSuchUpload."""
    key_a = f"mb-mpu-xkey/{uuid.uuid4()}.bin"
    key_b = f"mb-mpu-xkey/{uuid.uuid4()}.bin"
    mp = s3.create_multipart_upload(Bucket=BUCKET, Key=key_a)
    upload_id = mp["UploadId"]
    try:
        with pytest.raises(ClientError) as exc:
            s3.upload_part(
                Bucket=BUCKET, Key=key_b, UploadId=upload_id,
                PartNumber=1, Body=b"x" * (5 * 1024 * 1024),
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchUpload"
    finally:
        try:
            s3.abort_multipart_upload(Bucket=BUCKET, Key=key_a, UploadId=upload_id)
        except ClientError:
            pass


def test_multipart_roundtrip_on_secondary_bucket(s3):
    """BUCKET_2 is fully functional for multipart, not just single-shot ops."""
    key = f"mb-mpu-b2/{uuid.uuid4()}.bin"
    body = b"Y" * (5 * 1024 * 1024 + 5)
    mp = s3.create_multipart_upload(Bucket=BUCKET_2, Key=key)
    try:
        part = s3.upload_part(
            Bucket=BUCKET_2, Key=key, UploadId=mp["UploadId"],
            PartNumber=1, Body=body,
        )
        s3.complete_multipart_upload(
            Bucket=BUCKET_2, Key=key, UploadId=mp["UploadId"],
            MultipartUpload={"Parts": [{"ETag": part["ETag"], "PartNumber": 1}]},
        )
        assert s3.get_object(Bucket=BUCKET_2, Key=key)["Body"].read() == body
    finally:
        try:
            s3.delete_object(Bucket=BUCKET_2, Key=key)
        except ClientError:
            pass
