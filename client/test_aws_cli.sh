#!/usr/bin/env bash
# End-to-end test: aws CLI against a local buckets at http://localhost:9000.
#
# Verifies the "operator runs buckets in Docker, then uses plain aws s3 CLI"
# story works without any custom client. Roundtrips:
#   - 1 KiB object  (single PUT path)
#   - 20 MiB object (multipart, default 8 MiB chunks → 3 parts)
#
# Pre-req: buckets running on :9000 with valid AWS creds + ENCRYPTION_KEY.

set -euo pipefail

ENDPOINT="http://localhost:9000"
BUCKET="${TEST_BUCKET:?TEST_BUCKET must be set to a real S3 bucket}"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# aws CLI won't sign without creds; buckets discards sigs but the CLI checks
# locally. Use throwaway values.
export AWS_ACCESS_KEY_ID="tinfoil-cli"
export AWS_SECRET_ACCESS_KEY="tinfoil-cli"
export AWS_DEFAULT_REGION="us-east-2"
export AWS_REQUEST_CHECKSUM_CALCULATION="when_required"
export AWS_RESPONSE_CHECKSUM_VALIDATION="when_required"

# Force path-style addressing AND sequential multipart — buckets requires
# parts in order because the AES-GCM cipher state has to advance linearly.
AWS="aws --endpoint-url $ENDPOINT --no-verify-ssl"
aws configure set default.s3.addressing_style path
# Sequential uploads — the encryption client's AES-GCM state advances linearly.
aws configure set default.s3.max_concurrent_requests 1
# Disable ranged GET — the encryption client can't decrypt arbitrary ranges.
# 5 TiB exceeds S3's max object size, so all downloads stay single-request.
aws configure set default.s3.multipart_threshold 5TB

echo "--- 1 KiB roundtrip (single PUT) ---"
head -c 1024 /dev/urandom > "$TMPDIR/small.bin"
EXPECT=$(shasum -a 256 "$TMPDIR/small.bin" | awk '{print $1}')
$AWS s3 cp "$TMPDIR/small.bin" "s3://$BUCKET/aws-cli-test/small.bin"
$AWS s3 cp "s3://$BUCKET/aws-cli-test/small.bin" "$TMPDIR/small.out"
ACTUAL=$(shasum -a 256 "$TMPDIR/small.out" | awk '{print $1}')
if [ "$EXPECT" != "$ACTUAL" ]; then
  echo "  FAIL: sha mismatch ($EXPECT vs $ACTUAL)"; exit 1
fi
echo "  OK (sha=${EXPECT:0:16}...)"

echo "--- 20 MiB roundtrip (multipart, default 8 MiB chunks) ---"
head -c $((20 * 1024 * 1024)) /dev/urandom > "$TMPDIR/big.bin"
EXPECT=$(shasum -a 256 "$TMPDIR/big.bin" | awk '{print $1}')
$AWS s3 cp "$TMPDIR/big.bin" "s3://$BUCKET/aws-cli-test/big.bin"
$AWS s3 cp "s3://$BUCKET/aws-cli-test/big.bin" "$TMPDIR/big.out"
ACTUAL=$(shasum -a 256 "$TMPDIR/big.out" | awk '{print $1}')
if [ "$EXPECT" != "$ACTUAL" ]; then
  echo "  FAIL: sha mismatch ($EXPECT vs $ACTUAL)"; exit 1
fi
echo "  OK (sha=${EXPECT:0:16}...)"

echo "--- aws s3 ls ---"
$AWS s3 ls "s3://$BUCKET/aws-cli-test/" || true

echo "--- cleanup ---"
$AWS s3 rm "s3://$BUCKET/aws-cli-test/small.bin"
$AWS s3 rm "s3://$BUCKET/aws-cli-test/big.bin"

echo "ALL OK"
