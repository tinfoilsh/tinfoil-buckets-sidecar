# Using the AWS CLI with tinfoil-buckets

Run buckets in Docker, point `aws` at it. Works for `cp` / `ls` / `rm` / multipart.

## Start the sidecar

```
docker run --rm -d --name buckets -p 9000:9000 \
  -e AWS_REGION=us-east-2 \
  -e AWS_ACCESS_KEY_ID=<aws key> \
  -e AWS_SECRET_ACCESS_KEY=<aws secret> \
  -e ENCRYPTION_KEY=<base64 32-byte AES key> \
  ghcr.io/tinfoilsh/tinfoil-buckets-sidecar:latest
```

## Configure the CLI

Add to `~/.aws/config` (or a dedicated profile via `[profile tinfoil]`):

```
[default]
s3 =
    addressing_style = path
    max_concurrent_requests = 1
    multipart_threshold = 5TB
```

| Key | Why |
|---|---|
| `addressing_style = path` | We don't support virtual-hosted-style routing. |
| `max_concurrent_requests = 1` | AES-GCM cipher state has to advance linearly — out-of-order parts → 400. |
| `multipart_threshold = 5TB` | Disables ranged-GET downloads. The encryption client can't decrypt arbitrary ranges. |

Throwaway creds for the CLI (the sidecar discards sigv4 anyway, but the CLI refuses to send unsigned requests):

```
export AWS_ACCESS_KEY_ID=tinfoil-cli
export AWS_SECRET_ACCESS_KEY=tinfoil-cli
export AWS_DEFAULT_REGION=us-east-2
```

## Roundtrip

```
aws --endpoint-url http://localhost:9000 s3 cp ./local.bin s3://bucket/key.bin
aws --endpoint-url http://localhost:9000 s3 cp s3://bucket/key.bin ./roundtrip.bin
aws --endpoint-url http://localhost:9000 s3 ls s3://bucket/
```

The bucket name in the URL determines which S3 bucket the sidecar routes to — point it at any bucket your AWS credentials can reach.

See `client/test_aws_cli.sh` for a full PUT/GET/multipart/ls/rm script.
