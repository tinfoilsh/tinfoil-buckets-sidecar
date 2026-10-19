# Tinfoil Buckets Sidecar

An S3-compatible server that runs as a second container inside a Tinfoil enclave. It encrypts every object with the Amazon S3 Encryption Client before writing it to S3 and decrypts it on read, so S3 only sees ciphertext. See [Persistent storage](https://docs.tinfoil.sh/containers/persistent-storage) to deploy it, the [CLI](guides/cli.md) and [SDK](guides/sdk.md) guides to point clients at it, and [local decrypt](guides/local-decrypt.md) to read objects straight from S3 without the sidecar.

## Design Decisions (constraints)

> [!WARNING]
> Differences from standard S3:
>
> - Path-style URLs only (`forcePathStyle: true` or equivalent).
> - The bucket comes from the request path, so IAM decides which buckets are reachable.
> - No authentication: SigV4 signatures are accepted and discarded.
> - Sequential multipart only (`max_concurrency=1`). Out-of-order parts return `400 InvalidPart`.
> - Non-last parts must be 16-byte aligned. A misaligned part returns `400 InvalidArgument` on the next `UploadPart`, when the sidecar learns it was not the last.
> - Parts are capped at 1 GiB. Multipart state lives in sidecar memory: a restart loses in-progress uploads, and uploads older than an hour are aborted.
> - No ranged GETs (`400 InvalidArgument`).
> - GETs buffer the whole object to verify the authentication tag before sending any byte. `BUFFER_SIZE` sets the cap (default 1 GiB, max 64 GiB); larger objects return `413 EntityTooLarge`.

`DANGEROUS_DELAYED_AUTH=true` streams GETs of any size before the tag is verified and appends a 4-byte marker to every GET body: `TFOK` if the object verified, `TFNG` if not. Read with `verified_get` or `verified_iter` in [`client/tinfoil_client.py`](client/tinfoil_client.py), which strip and check the marker; any other client receives it as the last 4 bytes of the body and must check it itself.

## Configure

Create `.env` in the project root:

```
ENCRYPTION_KEY=<base64 32-byte AES-256 key>     # openssl rand -base64 32
AWS_REGION=us-east-2
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
PORT=9000
```

### Required AWS permissions

Grant the credentials these permissions on every bucket the sidecar should reach:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR-BUCKET-1/*",
        "arn:aws:s3:::YOUR-BUCKET-2/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:ListBucketMultipartUploads",
        "s3:GetBucketLocation"
      ],
      "Resource": ["arn:aws:s3:::YOUR-BUCKET-1", "arn:aws:s3:::YOUR-BUCKET-2"]
    }
  ]
}
```

### Multitenant mode

With `MULTITENANT=true`, one sidecar serves tenants with separate keys and ignores `ENCRYPTION_KEY`. Every request must carry:

- `X-Tinfoil-Tenant-Id`: matches `[A-Za-z0-9_-]{1,64}`. Objects are stored under `<tenantId>/<key>`, listings strip the prefix, and another tenant's `uploadId` returns `404 NoSuchUpload`.
- `X-Tinfoil-Encryption-Key`: the base64 32-byte AES-256 key for the object. A key that cannot decrypt the object returns `400 DecryptionFailed`, so the caller tracks which key encrypted which object.

The sidecar trusts both headers; the caller must authenticate tenants.

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md).

## Reporting Vulnerabilities

Please report security vulnerabilities by either:

- Emailing [security@tinfoil.sh](mailto:security@tinfoil.sh)
- Opening an issue on GitHub on this repository

We aim to respond to (legitimate) security reports within 24 hours.
