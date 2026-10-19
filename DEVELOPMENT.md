# Development

Create `.env` as described in [Configure](README.md#configure), then run the sidecar on port 9000:

```bash
mvn compile exec:java
```

Or in Docker:

```bash
docker build -t tinfoil-buckets-sidecar . && docker run --rm -p 9000:9000 -v $(pwd)/.env:/app/.env tinfoil-buckets-sidecar
```

## Test

The tests in `client/` run against a sidecar on `localhost:9000` and a real bucket your AWS credentials can reach (`TEST_BUCKET`, default `test-bucket-sidecar-1`).

```bash
python3 -m venv client/.venv
client/.venv/bin/pip install -r client/requirements.txt pytest requests
client/.venv/bin/pytest -v client/test_s3_compat.py
```

Cross-bucket routing needs a second bucket (`TEST_BUCKET_2`, default `test-bucket-sidecar-2`):

```bash
client/.venv/bin/pytest -v client/test_multi_bucket.py
```

Mode-specific tests are skipped unless you start the sidecar in the matching mode and tell the tests about it:

```bash
# 413 when an object exceeds BUFFER_SIZE
BUFFER_SIZE=10485760 mvn compile exec:java
SIDECAR_BUFFER_SIZE=10485760 client/.venv/bin/pytest -v client/test_s3_compat.py::test_buffer_exceeded_returns_413

# Streaming GETs with the delayed-auth marker
DANGEROUS_DELAYED_AUTH=true mvn compile exec:java
SIDECAR_DELAYED_AUTH=true client/.venv/bin/pytest -v client/test_s3_compat.py -k delayed_auth

# Multitenant mode
MULTITENANT=true mvn compile exec:java
SIDECAR_MULTITENANT=true client/.venv/bin/pytest -v client/test_multitenant.py
```

`client/test_aws_cli.sh` runs the same roundtrips with the AWS CLI.
