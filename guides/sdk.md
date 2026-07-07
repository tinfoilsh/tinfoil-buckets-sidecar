# Using an S3 SDK with tinfoil-buckets-sidecar

Same shape as the [CLI guide](cli.md): run buckets in Docker, point any S3 SDK at it.
The same three constraints apply, expressed in code instead of a config file.

## boto3 (Python)

```python
import boto3
from botocore.config import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    region_name="us-east-2",
    aws_access_key_id="tinfoil-sidecar",
    aws_secret_access_key="tinfoil-sidecar",
    config=Config(
        s3={"addressing_style": "path"},
    ),
)

# For high-level uploads/downloads, force sequential + single-request.
from boto3.s3.transfer import TransferConfig
tc = TransferConfig(
    max_concurrency=1,                 # sequential multipart upload
    multipart_threshold=5 * 1024**4,   # 5 TiB → no ranged GET on download
)
s3.upload_file("./local.bin", "bucket", "key.bin", Config=tc)
s3.download_file("bucket", "key.bin", "./roundtrip.bin", Config=tc)
```

If you drive multipart manually (`create_multipart_upload` + `upload_part` + `complete_multipart_upload`), **send non-last parts in multiples of 16 bytes**. The encryption client refuses unaligned non-last parts; we return a `400 InvalidArgument` naming the offending partNumber.

## Java SDK v2

```java
S3Client s3 = S3Client.builder()
    .endpointOverride(URI.create("http://localhost:9000"))
    .region(Region.US_EAST_2)
    .forcePathStyle(true)
    .credentialsProvider(StaticCredentialsProvider.create(
        AwsBasicCredentials.create("tinfoil-sidecar", "tinfoil-sidecar")))
    .build();
```

For high-level transfers (`S3TransferManager`), disable parallelism and set the multipart threshold above the largest object you'll handle.

## Anything else (Go, .NET, Node, …)

Three knobs to find in your SDK of choice:

| Knob | Setting |
|---|---|
| Endpoint | `http://localhost:9000` (or wherever you run the sidecar) |
| Path-style / `forcePathStyle` | enabled |
| Multipart upload concurrency | **1** (sequential parts) |
| Multipart download / ranged GET | **disabled** (raise threshold beyond your largest object, or use single GET) |

The endpoint discards sigv4 signatures, so any throwaway credentials work — SDKs just need *some* creds to sign with.

## Caveats

- **Bucket from the URL.** The bucket name in the request path determines which S3 bucket the sidecar routes to.
- **GET buffers in memory.** Default cap is 1 GiB; raise with `BUFFER_SIZE` env (up to 64 GiB). For larger objects, see `DANGEROUS_DELAYED_AUTH` in the main [README](../README.md#design-decisions-constraints).
