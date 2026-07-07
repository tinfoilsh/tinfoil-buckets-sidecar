Tinfoil Buckets Sidecar.

Simple server that exposes an S3 API to the local network. Internally uses S3 encrypted client. Encrypts & decrypts. Made to run as a side-car container on an enclave.

> [!WARNING]
> Differences from normal S3:
>
> - Sequential multipart only (`max_concurrency=1`)
> - Non-last parts must be 16-byte aligned
> - No ranged GETs
> - GETs buffer the whole object (default 1 GiB; raise to 64 GiB or stream via `DANGEROUS_DELAYED_AUTH=true`)

## Usage

1. Inside a tinfoil secure enclave. See our [persistent-storage-example](https://github.com/tinfoilsh/tinfoil-persistent-storage-example).
2. Spun up locally inside a docker container, then pointing the aws CLI or any S3 SDK towards `localhost:9000`. See [`guides/cli.md`](guides/cli.md) and [`guides/sdk.md`](guides/sdk.md).
3. For local reading of encrypted files directly from S3 (no sidecar), use the teensie Python decrypt helper. See [`guides/local-decrypt.md`](guides/local-decrypt.md). TODO: copy this from persistent storage into the tinfoil CLI.

## Notes

- **Path-style only.** Configure your S3 SDK with `forcePathStyle: true` (or equivalent). Virtual-hosted (`bucket.s3.amazonaws.com`) URLs are not supported.
- **Bucket comes from the request URL.** The sidecar routes to whatever bucket the client specifies in the path (`s3://bucket/key`). IAM is the enforcement point for which buckets are reachable — see [Required AWS permissions](#required-aws-permissions).
- **No auth.** sigv4 signatures from clients are accepted and discarded.
- When GET-ing large files, users need to use a special client. Aside from that, any S3 sdk should work.

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

The sidecar uses your AWS credentials directly (the `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` above) and whatever IAM permissions those credentials
carry. Attach a policy to the IAM user or role with these permissions on your
bucket(s). List multiple `Resource` ARNs to grant access to more than one
bucket under a single set of credentials:

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
      "Resource": [
        "arn:aws:s3:::YOUR-BUCKET-1",
        "arn:aws:s3:::YOUR-BUCKET-2"
      ]
    }
  ]
}
```

### Multitenant mode

For deployments where multiple tenants share one sidecar (each with their own
encryption key), set `MULTITENANT=true`. In this mode:

- `ENCRYPTION_KEY` is **not** required (and is ignored if set) — the key arrives per-request.
- Every request must carry two headers:
  - `X-Tinfoil-Tenant-Id: <id>` — must match `[A-Za-z0-9_-]{1,64}`.
  - `X-Tinfoil-Encryption-Key: <base64 32-byte AES-256 key>` — encrypts/decrypts the object.
- Objects are namespaced in the backing bucket under `<tenantId>/<key>`. The
  prefix is invisible to callers — `ListObjectsV2` and `ListMultipartUploads`
  return user-facing keys with the prefix stripped.
- Multipart sessions are scoped to `(tenantId, uploadId)`; a different tenant
  using the same `uploadId` gets `NoSuchUpload`.
- Per-tenant encryption clients are cached (LRU, 256 entries) so cold-cache
  requests pay a one-time build cost; subsequent requests reuse the cached client.
- The sidecar trusts both headers — auth is the callers responsibility

Tenant ID and encryption key are decoupled so a single tenant can rotate keys
(or hold multiple keys for different objects) without changing namespace. The
caller is responsible for tracking which key decrypts which object: if the
wrong key arrives for a given object, the sidecar returns
`400 DecryptionFailed`.

## Run

- Local: `mvn compile exec:java`
- Docker: `docker build -t tinfoil-buckets-sidecar . && docker run --rm -p 9000:9000 -v $(pwd)/.env:/app/.env tinfoil-buckets-sidecar`

## Test

`client/` contains the python S3 sdk and the pytest suite.

Tests target whatever bucket `TEST_BUCKET` points at. The bucket must be accessible with your AWS credentials.

```
TEST_BUCKET=your-bucket client/.venv/bin/pytest -v client/test_s3_compat.py
```

Default suite (sidecar in default buffered mode, any `BUFFER_SIZE`):

```
client/.venv/bin/pytest -v client/test_s3_compat.py
```

### Mode-specific tests (opt-in)

Some tests are gated on env vars so they only run when the sidecar is in the matching mode.

**Buffer-exceeded path** — verify the `413 EntityTooLarge` response when an object is bigger than `BUFFER_SIZE`. Start the sidecar with a small buffer (here, 10 MiB):

```
BUFFER_SIZE=10485760 mvn compile exec:java
```

In another shell, tell the test what cap the sidecar is using:

```
SIDECAR_BUFFER_SIZE=10485760 client/.venv/bin/pytest -v client/test_s3_compat.py::test_buffer_exceeded_returns_413
```

**Delayed-auth mode** — verify streaming + trailer behavior. Start the sidecar with delayed auth on:

```
DANGEROUS_DELAYED_AUTH=true mvn compile exec:java
```

In another shell:

```
SIDECAR_DELAYED_AUTH=true client/.venv/bin/pytest -v client/test_s3_compat.py -k delayed_auth
```

## Design Decisions (constraints)

1. Force the client to send parts sequentially on multipart `(max_concurrency=1)`. This is because the encryption client iterates through input sequentially - it needs linear, in order access to the input to do encryption.
2. The low-level multipart upload exposed by S3 sdk requires that you mark the final uploadPart w/ `isLastPart=true`. The S3 protocol doesn't tell us this - we only know once the user sends the `CompleteMultipartUpload` that it was the last, or if another part arrives that this wasn't the last.
   1. This means that before we upload part N we need to recieve part N+1.
3. Normally, on upload of each part S3, an `ETag` is returned that can be used to check the validity of a part in the future. `CompleteMultipartUpload` requires the `ETag` of the last part in the request. This means that for the penultimate part the server enters a deadlock where it's waiting for the client, but the client can't send the complete.
   1. For this reason, we have decided to completely do false ETags.
   2. Also the client can't even use `ETags`, as they're only useful if you hold the ciphertext. We'll just keep track of these on the server.
4. Multi-part constraints
   1. Setting a 1GB per part cap for now, as we have to hold the full thing in the server.
   2. Can have as many active sessions as you want. User beware.
5. Multi-part state is local to the server, because we need to have the running AES-GCM cipher state so we can continue uploading. If this ever restarts, we lose this session, so there's no point in trying to use the S3 as the SSOT for session state.
6. GET needs to buffer until the end so we can check the authentication tag. The user can set this with an environment var of bufferSize. The user is responsible for memory consumption.
7. In `DANGEROUS_DELAYED_AUTH=true` mode only, GET responses have **4 extra bytes appended to the body** (`TFOK` for success, `TFNG` for auth failure). We tried HTTP trailers but Jetty drops chunked responses larger than ~64 KiB when trailer fields are configured. Default (buffered) mode appends no marker — bytes are clean plaintext. Use `verified_get` / `verified_iter` in `client/tinfoil_client.py` to strip+verify the marker in delayed-auth mode; plain boto3 in delayed-auth mode receives the marker as the last 4 bytes of the body (operator's responsibility).
   1. The implementation of this looks like:
      1. By default, we are in buffer mode. The environment variable defaults to 1gb, but can go up to 64gb reflecting the underlying clients maxBufferSize (or whatever it's called). If users try and GET something larger than bufferSize, it returns a helpful error message
      2. If users need to get something larger than 64Gb, or don't want memory pressure in their server, then they can pass delayedAuth. This turns on delayedAuth on the client, and streams everything. In this mode, the client is responsible for doing a special get that handles the trailer if the data turns out to be bad. This is a user-beware feature.
8. non-last part uploads must be in multiples of 16 bytes. We return an error if one is not. For the high level multipart apis (eg client has big object, just split it up however) this should be fine - they typically use 8MiB
   1. The error is: `400 InvalidArgument` on the _next_ `UploadPart` call (that's when we know it's not last)
