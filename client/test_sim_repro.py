"""Reproduce the multipart failure that the persistent-storage-example `sim` hits
against the deployed buckets sidecar. Run against a local buckets on :9000.

Mirrors sim.py + storage.py as closely as possible:
- same boto3 client config (path-style, dummy creds, hardcoded endpoint)
- ContentType="application/octet-stream" on create_multipart_upload
- bytearray accumulation, flush at PART_SIZE_BYTES
- multiple sequential phases, each its own multipart upload
- trailing small final part
"""

import hashlib
import os
import time
import traceback

import boto3
from botocore.config import Config

ENDPOINT = "http://localhost:9000"
BUCKET = os.environ.get("TEST_BUCKET", "test-bucket-sidecar-1")
PART_SIZE_BYTES = 5 * 1024 * 1024  # match sim's PART_SIZE_MB=5 default

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    region_name="us-east-2",
    config=Config(s3={"addressing_style": "path"}),
    aws_access_key_id="tinfoil-sidecar",
    aws_secret_access_key="tinfoil-sidecar",
)


def flush_part(key, upload_id, part_number, buf, parts, written, is_last):
    # Non-last parts must be 16-byte aligned (AES block); last part can be any size.
    if is_last:
        body = bytes(buf)
        buf.clear()
    else:
        aligned = (len(buf) // 16) * 16
        body = bytes(buf[:aligned])
        del buf[:aligned]
    print(f"  flushing part {part_number} ({len(body)} bytes, last={is_last})", flush=True)
    written.update(body)
    resp = s3.upload_part(
        Bucket=BUCKET, Key=key, UploadId=upload_id, PartNumber=part_number, Body=body
    )
    parts.append({"PartNumber": part_number, "ETag": resp["ETag"]})


def run_phase(phase_idx, run_id):
    key = f"persistent-storage/{run_id}/phase-{phase_idx}.bin"
    print(f"[phase {phase_idx}] create_multipart key={key}", flush=True)
    uid = s3.create_multipart_upload(
        Bucket=BUCKET, Key=key, ContentType="application/octet-stream"
    )["UploadId"]
    print(f"[phase {phase_idx}] uploadId={uid[:32]}...", flush=True)

    parts = []
    buf = bytearray()
    part_number = 0
    written = hashlib.sha256()

    bytes_per_step = 5000
    steps = 2500
    for _ in range(steps):
        buf.extend(os.urandom(bytes_per_step))
        if len(buf) >= PART_SIZE_BYTES:
            part_number += 1
            flush_part(key, uid, part_number, buf, parts, written, is_last=False)
        time.sleep(0.0005)

    if buf or not parts:
        part_number += 1
        flush_part(key, uid, part_number, buf, parts, written, is_last=True)

    print(f"[phase {phase_idx}] complete ({len(parts)} parts)", flush=True)
    s3.complete_multipart_upload(
        Bucket=BUCKET, Key=key, UploadId=uid, MultipartUpload={"Parts": parts}
    )
    expected = written.hexdigest()
    print(f"[phase {phase_idx}] uploaded sha256={expected[:16]}...", flush=True)

    # Roundtrip: GET the object, hash its plaintext, compare.
    got = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    actual = hashlib.sha256(got).hexdigest()
    if actual != expected:
        raise AssertionError(
            f"sha256 mismatch! expected {expected} got {actual} "
            f"(expected size={steps*bytes_per_step}, got size={len(got)})"
        )
    print(f"[phase {phase_idx}] roundtrip OK ({len(got)} bytes)", flush=True)


def main():
    run_id = time.strftime("%Y-%m-%dT%H-%M-%SZ", time.gmtime())
    print(f"run_id={run_id}", flush=True)
    for phase_idx in range(3):
        try:
            run_phase(phase_idx, run_id)
        except Exception:
            print(f"!!! PHASE {phase_idx} FAILED", flush=True)
            traceback.print_exc()
            return 1
    print("ALL PHASES OK", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
