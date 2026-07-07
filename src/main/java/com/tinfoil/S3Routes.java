package com.tinfoil;

import java.io.ByteArrayInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.crypto.BadPaddingException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import com.tinfoil.TenantResolver.TenantCtx;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.HandlerType;
import io.javalin.http.HttpStatus;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.DeletedObject;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.ListPartsResponse;
import software.amazon.awssdk.services.s3.model.MultipartUpload;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.Part;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.SdkPartType;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.encryption.s3.S3EncryptionClientException;

public class S3Routes {
    private static final int GCM_TAG_BYTES = 16;
    public static final long MAX_PART_BYTES = 1024L * 1024L * 1024L;
    private static final byte[] MARKER_OK = {'T', 'F', 'O', 'K'};
    private static final byte[] MARKER_FAIL = {'T', 'F', 'N', 'G'};
    public static final int MARKER_BYTES = 4;
    private static final long MULTIPART_SESSION_TTL_SECONDS = 3600;
    private static final long MULTIPART_GC_PERIOD_SECONDS = 300;

    private final TenantResolver resolver;
    private final S3Client housekeepingClient; 
    private final Region region;
    private final boolean delayedAuth;
    private final ConcurrentHashMap<String, MultipartSession> sessions = new ConcurrentHashMap<>();
    private final ScheduledExecutorService gc = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "tinfoil-mpu-gc");
        t.setDaemon(true);
        return t;
    });

    public S3Routes(TenantResolver resolver, S3Client housekeepingClient, Config config) {
        this.resolver = resolver;
        this.housekeepingClient = housekeepingClient;
        this.region = config.region();
        this.delayedAuth = config.delayedAuth();
    }

    public void shutdown() {
        gc.shutdownNow();
    }

    public void register(Javalin app) {
        app.put("/{bucket}/<key>", this::handlePut);
        app.get("/{bucket}/<key>", this::handleGet);
        app.head("/{bucket}/<key>", this::handleHead);
        app.delete("/{bucket}/<key>", this::handleDelete);
        app.post("/{bucket}/<key>", this::handlePost);
        app.get("/{bucket}", this::handleBucketGet);
        app.get("/{bucket}/", this::handleBucketGet);
        app.post("/{bucket}", this::handleBucketPost);
        app.post("/{bucket}/", this::handleBucketPost);
        app.head("/{bucket}", this::handleBucketHead);
        app.head("/{bucket}/", this::handleBucketHead);

        gc.scheduleAtFixedRate(this::sweepSessions,
                MULTIPART_GC_PERIOD_SECONDS, MULTIPART_GC_PERIOD_SECONDS, TimeUnit.SECONDS);

        app.exception(S3Exception.class, (e, ctx) -> {
            e.printStackTrace();
            handleS3Exception(e, ctx);
        });
        app.exception(S3EncryptionClientException.class, (e, ctx) -> {
            e.printStackTrace();
            if (e.getCause() instanceof S3Exception s3e) {
                handleS3Exception(s3e, ctx);
            } else if (isAeadAuthFailure(e)) {
                // GCM tag mismatch: the supplied key cannot decrypt this object. Safe to surface
                writeS3Error(ctx, 400, "DecryptionFailed",
                        "Decryption failed for this object. The provided encryption key "
                        + "cannot decrypt it (the object may have been encrypted with a "
                        + "different key).");
            } else if (e.getMessage() != null
                    && e.getMessage().contains("exceeds the maximum buffer size")) {
                writeS3Error(ctx, 413, "EntityTooLarge",
                        "Object exceeds the sidecar's BUFFER_SIZE. "
                        + "Raise BUFFER_SIZE (up to 64 GiB), or set "
                        + "DANGEROUS_DELAYED_AUTH=true to stream (supports arbitrarily large objects.)");
            } else {
                writeS3Error(ctx, 500, "InternalError", e.getMessage());
            }
        });
        app.exception(Exception.class, (e, ctx) -> {
            e.printStackTrace();
            writeS3Error(ctx, 500, "InternalError", e.getMessage());
        });
    }

    // --- Dispatchers ---------------------------------------------------------

    private void handlePut(Context ctx) {
        String uploadId = ctx.queryParam("uploadId");
        if (uploadId != null) {
            uploadPart(ctx, uploadId);
        } else {
            putObject(ctx);
        }
    }

    private void handleDelete(Context ctx) {
        String uploadId = ctx.queryParam("uploadId");
        if (uploadId != null) {
            abortMultipartUpload(ctx, uploadId);
        } else {
            deleteObject(ctx);
        }
    }

    private void handlePost(Context ctx) {
        if (ctx.queryParamMap().containsKey("uploads")) {
            createMultipartUpload(ctx);
        } else if (ctx.queryParam("uploadId") != null) {
            completeMultipartUpload(ctx, ctx.queryParam("uploadId"));
        } else {
            writeS3Error(ctx, 400, "InvalidRequest", "Unsupported POST operation.");
        }
    }

    // --- Single-shot object operations ---------------------------------------

    private void putObject(Context ctx) {
        TenantCtx tenant = resolver.resolve(ctx);
        if (tenant == null) return;
        String userKey = ctx.pathParam("key");
        String bucket = ctx.pathParam("bucket");

        ResolvedBody rb = resolveBody(ctx, 411, "MissingContentLength",
                "Content-Length header is required for PutObject.");
        if (rb == null) return;

        PutObjectRequest.Builder b = PutObjectRequest.builder().bucket(bucket).key(tenant.s3Key(userKey));
        String contentType = ctx.header("Content-Type");
        if (contentType != null) b.contentType(contentType);
        Map<String, String> userMeta = extractUserMetadata(ctx);
        if (!userMeta.isEmpty()) b.metadata(userMeta);

        PutObjectResponse resp = tenant.client().putObject(b.build(),
                RequestBody.fromInputStream(rb.stream(), rb.length()));
        if (resp.eTag() != null) ctx.header("ETag", resp.eTag());
        ctx.status(HttpStatus.OK);
    }

    private void handleGet(Context ctx) {
        String uploadId = ctx.queryParam("uploadId");
        if (uploadId != null) {
            listParts(ctx, uploadId);
            return;
        }
        if (ctx.header("Range") != null) {
            writeS3Error(ctx, 400, "InvalidArgument",
                    "Range requests are not supported (encrypted objects can't be read in slices).");
            return;
        }
        TenantCtx tenant = resolver.resolve(ctx);
        if (tenant == null) return;
        String userKey = ctx.pathParam("key");
        String bucket = ctx.pathParam("bucket");
        String s3K = tenant.s3Key(userKey);

        // In DEFAULT (buffered) mode: the encryption client verifies the GCM
        // tag before any byte is released, so a successful response is
        // already trustworthy. No marker is appended — clients get clean
        // plaintext bytes exactly as stored.
        //
        // In DANGEROUS_DELAYED_AUTH mode: plaintext streams to the client
        // before the GCM tag is verified. We append a 4-byte marker to the
        // body so trailer-aware clients can detect tampering:
        //   "TFOK" on success, "TFNG" on auth failure at end-of-stream.
        // (We originally tried HTTP trailers but Jetty drops the connection
        // mid-stream when trailers are configured on chunked responses
        // larger than a few KiB.)
        try {
            tenant.client().getObject(b -> b.bucket(bucket).key(s3K), (response, in) -> {
                if (response.contentType() != null) ctx.contentType(response.contentType());
                if (response.eTag() != null) ctx.header("ETag", response.eTag());
                if (response.lastModified() != null) {
                    ctx.header("Last-Modified", java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME
                            .format(response.lastModified().atZone(java.time.ZoneOffset.UTC)));
                }
                writeUserMetadataHeaders(ctx, response.metadata());
                try (var stream = in) {
                    stream.transferTo(ctx.outputStream());
                }
                return response;
            });
            // Auth verified — append the success marker (delayed-auth only).
            if (delayedAuth) {
                try { ctx.outputStream().write(MARKER_OK); } catch (java.io.IOException ignored) {}
            }
        } catch (S3EncryptionClientException e) {
            if (e.getCause() instanceof S3Exception) throw e;
            if (!ctx.res().isCommitted()) {
                // No bytes flowed yet (buffered mode failed before transfer) —
                // fail with a clean S3 XML error response.
                throw e;
            }
            // Mid-stream auth failure (only possible in delayed-auth mode) —
            // append the fail marker for trailer-aware clients. Trailer-blind
            // clients accept the risk.
            try { ctx.outputStream().write(MARKER_FAIL); } catch (java.io.IOException ignored) {}
        }
    }

    private void handleHead(Context ctx) {
        TenantCtx tenant = resolver.resolve(ctx);
        if (tenant == null) return;
        String userKey = ctx.pathParam("key");
        String bucket = ctx.pathParam("bucket");
        HeadObjectResponse resp = tenant.client().headObject(b -> b
                .bucket(bucket).key(tenant.s3Key(userKey)));
        // AES-GCM (v4 default) appends a 16-byte authentication tag to the ciphertext.
        long len = Math.max(0, resp.contentLength() - GCM_TAG_BYTES);
        ctx.header("Content-Length", String.valueOf(len));
        if (resp.contentType() != null) ctx.contentType(resp.contentType());
        if (resp.eTag() != null) ctx.header("ETag", resp.eTag());
        if (resp.lastModified() != null) {
            ctx.header("Last-Modified", java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME
                    .format(resp.lastModified().atZone(java.time.ZoneOffset.UTC)));
        }
        writeUserMetadataHeaders(ctx, resp.metadata());
        ctx.status(HttpStatus.OK);
    }

    private void deleteObject(Context ctx) {
        TenantCtx tenant = resolver.resolve(ctx);
        if (tenant == null) return;
        String bucket = ctx.pathParam("bucket");
        tenant.client().deleteObject(b -> b.bucket(bucket).key(tenant.s3Key(ctx.pathParam("key"))));
        ctx.status(HttpStatus.NO_CONTENT);
    }

    // --- Bucket-level operations ---------------------------------------------

    private void handleBucketHead(Context ctx) {
        TenantCtx tenant = resolver.resolve(ctx);
        if (tenant == null) return;
        String bucket = ctx.pathParam("bucket");
        housekeepingClient.headBucket(b -> b.bucket(bucket));
        ctx.status(HttpStatus.OK);
    }

    private void handleBucketGet(Context ctx) {
        if ("2".equals(ctx.queryParam("list-type"))) {
            listObjectsV2(ctx);
        } else if (ctx.queryParamMap().containsKey("uploads")) {
            listMultipartUploads(ctx);
        } else if (ctx.queryParamMap().containsKey("location")) {
            getBucketLocation(ctx);
        } else {
            writeS3Error(ctx, 400, "InvalidRequest",
                    "Unsupported bucket GET operation.");
        }
    }

    private void handleBucketPost(Context ctx) {
        if (ctx.queryParamMap().containsKey("delete")) {
            deleteObjects(ctx);
        } else {
            writeS3Error(ctx, 400, "InvalidRequest",
                    "Unsupported bucket POST operation.");
        }
    }

    private void getBucketLocation(Context ctx) {
        ctx.contentType("application/xml");
        ctx.result("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                + xmlEscape(region.id())
                + "</LocationConstraint>");
    }

    private void deleteObjects(Context ctx) {
        TenantCtx tenant = resolver.resolve(ctx);
        if (tenant == null) return;
        String bucket = ctx.pathParam("bucket");
        List<ObjectIdentifier> userKeys = parseDeleteRequestKeys(ctx.bodyAsBytes());
        if (userKeys.isEmpty()) {
            writeS3Error(ctx, 400, "MalformedXML", "Delete request contained no keys.");
            return;
        }
        // Apply tenant prefix to each requested key for the upstream call.
        List<ObjectIdentifier> toDelete = new ArrayList<>(userKeys.size());
        for (ObjectIdentifier u : userKeys) {
            toDelete.add(ObjectIdentifier.builder().key(tenant.s3Key(u.key())).build());
        }
        DeleteObjectsResponse resp = tenant.client().deleteObjects(DeleteObjectsRequest.builder()
                .bucket(bucket)
                .delete(d -> d.objects(toDelete))
                .build());

        StringBuilder xml = new StringBuilder(256);
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?><DeleteResult>");
        if (resp.deleted() != null) {
            for (DeletedObject d : resp.deleted()) {
                xml.append("<Deleted><Key>").append(xmlEscape(tenant.stripPrefix(d.key()))).append("</Key></Deleted>");
            }
        }
        if (resp.errors() != null) {
            for (S3Error e : resp.errors()) {
                xml.append("<Error>");
                xml.append("<Key>").append(xmlEscape(tenant.stripPrefix(e.key()))).append("</Key>");
                if (e.code() != null) xml.append("<Code>").append(xmlEscape(e.code())).append("</Code>");
                if (e.message() != null) xml.append("<Message>").append(xmlEscape(e.message())).append("</Message>");
                xml.append("</Error>");
            }
        }
        xml.append("</DeleteResult>");
        ctx.contentType("application/xml");
        ctx.result(xml.toString());
    }

    private static List<ObjectIdentifier> parseDeleteRequestKeys(byte[] body) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            // Reject DOCTYPE declarations to prevent XXE (external entity expansion,
            // SSRF via SYSTEM URIs, billion-laughs DoS).
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(new ByteArrayInputStream(body));
            NodeList keyNodes = doc.getElementsByTagName("Key");
            List<ObjectIdentifier> keys = new ArrayList<>(keyNodes.getLength());
            for (int i = 0; i < keyNodes.getLength(); i++) {
                keys.add(ObjectIdentifier.builder().key(keyNodes.item(i).getTextContent()).build());
            }
            return keys;
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse DeleteObjects request: " + e.getMessage(), e);
        }
    }

    private void listMultipartUploads(Context ctx) {
        TenantCtx tenant = resolver.resolve(ctx);
        if (tenant == null) return;
        String bucket = ctx.pathParam("bucket");
        ListMultipartUploadsRequest.Builder req = ListMultipartUploadsRequest.builder().bucket(bucket);
        if (tenant.tenantId() != null) req.prefix(tenant.prefix());
        ListMultipartUploadsResponse resp = tenant.client().listMultipartUploads(req.build());
        StringBuilder xml = new StringBuilder(512);
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListMultipartUploadsResult>");
        xml.append("<Bucket>").append(xmlEscape(bucket)).append("</Bucket>");
        xml.append("<KeyMarker></KeyMarker>");
        xml.append("<UploadIdMarker></UploadIdMarker>");
        xml.append("<NextKeyMarker></NextKeyMarker>");
        xml.append("<NextUploadIdMarker></NextUploadIdMarker>");
        xml.append("<MaxUploads>").append(resp.maxUploads() != null ? resp.maxUploads() : 1000).append("</MaxUploads>");
        xml.append("<IsTruncated>").append(Boolean.TRUE.equals(resp.isTruncated())).append("</IsTruncated>");
        if (resp.uploads() != null) {
            for (MultipartUpload u : resp.uploads()) {
                xml.append("<Upload>");
                xml.append("<Key>").append(xmlEscape(tenant.stripPrefix(u.key()))).append("</Key>");
                xml.append("<UploadId>").append(xmlEscape(u.uploadId())).append("</UploadId>");
                if (u.initiated() != null) {
                    xml.append("<Initiated>").append(u.initiated().toString()).append("</Initiated>");
                }
                xml.append("<StorageClass>STANDARD</StorageClass>");
                xml.append("</Upload>");
            }
        }
        xml.append("</ListMultipartUploadsResult>");
        ctx.contentType("application/xml");
        ctx.result(xml.toString());
    }

    private void listParts(Context ctx, String uploadId) {
        TenantCtx tenant = resolver.resolve(ctx);
        if (tenant == null) return;
        String userKey = ctx.pathParam("key");
        String bucket = ctx.pathParam("bucket");
        MultipartSession session = lookupSession(ctx, tenant, uploadId);
        if (session == null) return;
        ListPartsResponse resp = tenant.client().listParts(ListPartsRequest.builder()
                .bucket(bucket).key(tenant.s3Key(userKey)).uploadId(uploadId).build());

        // Snapshot our locally-buffered "pending" part, if any, so we can surface
        // it in the listing. Upstream doesn't see it yet — we hold the most-recent
        // part until we know whether it's the last (see multipart design notes).
        Integer pendingNum = null;
        Integer pendingSize = null;
        String pendingEtag = null;
        session.lock.lock();
        try {
            if (session.pendingPartBytes != null) {
                pendingNum = session.pendingPartNumber;
                pendingSize = session.pendingPartBytes.length;
                pendingEtag = session.pendingPartEtag;
            }
        } finally {
            session.lock.unlock();
        }

        StringBuilder xml = new StringBuilder(512);
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListPartsResult>");
        xml.append("<Bucket>").append(xmlEscape(bucket)).append("</Bucket>");
        xml.append("<Key>").append(xmlEscape(userKey)).append("</Key>");
        xml.append("<UploadId>").append(xmlEscape(uploadId)).append("</UploadId>");
        xml.append("<PartNumberMarker>0</PartNumberMarker>");
        int nextMarker = resp.nextPartNumberMarker() != null ? resp.nextPartNumberMarker() : 0;
        if (pendingNum != null && pendingNum > nextMarker) nextMarker = pendingNum;
        xml.append("<NextPartNumberMarker>").append(nextMarker).append("</NextPartNumberMarker>");
        xml.append("<MaxParts>").append(resp.maxParts() != null ? resp.maxParts() : 1000).append("</MaxParts>");
        xml.append("<IsTruncated>").append(Boolean.TRUE.equals(resp.isTruncated())).append("</IsTruncated>");
        xml.append("<StorageClass>STANDARD</StorageClass>");
        if (resp.parts() != null) {
            for (Part p : resp.parts()) {
                xml.append("<Part>");
                xml.append("<PartNumber>").append(p.partNumber()).append("</PartNumber>");
                if (p.lastModified() != null) {
                    xml.append("<LastModified>").append(p.lastModified().toString()).append("</LastModified>");
                }
                if (p.eTag() != null) {
                    xml.append("<ETag>").append(xmlEscape(p.eTag())).append("</ETag>");
                }
                long size = p.size() != null ? Math.max(0, p.size() - GCM_TAG_BYTES) : 0;
                xml.append("<Size>").append(size).append("</Size>");
                xml.append("</Part>");
            }
        }
        if (pendingNum != null) {
            xml.append("<Part>");
            xml.append("<PartNumber>").append(pendingNum).append("</PartNumber>");
            if (pendingEtag != null) {
                xml.append("<ETag>").append(xmlEscape(pendingEtag)).append("</ETag>");
            }
            xml.append("<Size>").append(pendingSize).append("</Size>");
            xml.append("</Part>");
        }
        xml.append("</ListPartsResult>");
        ctx.contentType("application/xml");
        ctx.result(xml.toString());
    }

    private void sweepSessions() {
        Instant cutoff = Instant.now().minusSeconds(MULTIPART_SESSION_TTL_SECONDS);
        List<MultipartSession> expired = new ArrayList<>();
        for (MultipartSession s : sessions.values()) {
            if (s.createdAt.isBefore(cutoff)) expired.add(s);
        }
        for (MultipartSession s : expired) {
            if (sessions.remove(TenantCtx.sessionKeyFor(s.tenantId, s.uploadId)) == null) continue;
            String s3K = TenantCtx.s3KeyFor(s.tenantId, s.key);
            try {
                // Abort is not crypto-aware — housekeeping client works in either mode.
                housekeepingClient.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                        .bucket(s.bucket).key(s3K).uploadId(s.uploadId).build());
            } catch (Exception e) {
                System.err.println("multipart GC: abort failed for " + s.uploadId + ": " + e.getMessage());
            }
        }
    }

    private void listObjectsV2(Context ctx) {
        TenantCtx tenant = resolver.resolve(ctx);
        if (tenant == null) return;
        String bucket = ctx.pathParam("bucket");
        ListObjectsV2Request.Builder b = ListObjectsV2Request.builder().bucket(bucket);
        String userPrefix = ctx.queryParam("prefix");
        // In multitenant mode, force-scope the listing to <tenantId>/<userPrefix>.
        String upstreamPrefix = tenant.prefix() + (userPrefix != null ? userPrefix : "");
        if (!upstreamPrefix.isEmpty()) b.prefix(upstreamPrefix);
        String delimiter = ctx.queryParam("delimiter");
        if (delimiter != null) b.delimiter(delimiter);
        String maxKeys = ctx.queryParam("max-keys");
        if (maxKeys != null) b.maxKeys(Integer.parseInt(maxKeys));
        String continuationToken = ctx.queryParam("continuation-token");
        if (continuationToken != null) b.continuationToken(continuationToken);
        String startAfter = ctx.queryParam("start-after");
        if (startAfter != null) b.startAfter(tenant.s3Key(startAfter));

        ListObjectsV2Response resp = tenant.client().listObjectsV2(b.build());

        StringBuilder xml = new StringBuilder(512);
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        xml.append("<ListBucketResult>");
        xml.append("<Name>").append(xmlEscape(bucket)).append("</Name>");
        if (userPrefix != null) {
            xml.append("<Prefix>").append(xmlEscape(userPrefix)).append("</Prefix>");
        }
        if (delimiter != null) {
            xml.append("<Delimiter>").append(xmlEscape(delimiter)).append("</Delimiter>");
        }
        xml.append("<KeyCount>").append(resp.keyCount() != null ? resp.keyCount() : 0).append("</KeyCount>");
        xml.append("<MaxKeys>").append(resp.maxKeys() != null ? resp.maxKeys() : 1000).append("</MaxKeys>");
        xml.append("<IsTruncated>").append(Boolean.TRUE.equals(resp.isTruncated())).append("</IsTruncated>");
        if (continuationToken != null) {
            xml.append("<ContinuationToken>").append(xmlEscape(continuationToken)).append("</ContinuationToken>");
        }
        if (resp.nextContinuationToken() != null) {
            xml.append("<NextContinuationToken>").append(xmlEscape(resp.nextContinuationToken())).append("</NextContinuationToken>");
        }
        if (resp.contents() != null) {
            for (S3Object obj : resp.contents()) {
                xml.append("<Contents>");
                xml.append("<Key>").append(xmlEscape(tenant.stripPrefix(obj.key()))).append("</Key>");
                if (obj.lastModified() != null) {
                    xml.append("<LastModified>").append(obj.lastModified().toString()).append("</LastModified>");
                }
                if (obj.eTag() != null) {
                    xml.append("<ETag>").append(xmlEscape(obj.eTag())).append("</ETag>");
                }
                // Size is ciphertext size; subtract GCM tag for plaintext.
                long size = obj.size() != null ? Math.max(0, obj.size() - GCM_TAG_BYTES) : 0;
                xml.append("<Size>").append(size).append("</Size>");
                xml.append("<StorageClass>STANDARD</StorageClass>");
                xml.append("</Contents>");
            }
        }
        if (resp.commonPrefixes() != null) {
            for (CommonPrefix cp : resp.commonPrefixes()) {
                xml.append("<CommonPrefixes>");
                xml.append("<Prefix>").append(xmlEscape(tenant.stripPrefix(cp.prefix()))).append("</Prefix>");
                xml.append("</CommonPrefixes>");
            }
        }
        xml.append("</ListBucketResult>");

        ctx.contentType("application/xml");
        ctx.result(xml.toString());
    }

    // --- Multipart operations ------------------------------------------------

    private void createMultipartUpload(Context ctx) {
        TenantCtx tenant = resolver.resolve(ctx);
        if (tenant == null) return;
        String userKey = ctx.pathParam("key");
        String bucket = ctx.pathParam("bucket");
        CreateMultipartUploadRequest.Builder b = CreateMultipartUploadRequest.builder()
                .bucket(bucket).key(tenant.s3Key(userKey));
        String contentType = ctx.header("Content-Type");
        if (contentType != null) b.contentType(contentType);
        Map<String, String> userMeta = extractUserMetadata(ctx);
        if (!userMeta.isEmpty()) b.metadata(userMeta);
        CreateMultipartUploadResponse resp = tenant.client().createMultipartUpload(b.build());
        String uploadId = resp.uploadId();
        // Sessions store the user-facing key; we re-prefix at S3-op boundaries.
        sessions.put(tenant.sessionKey(uploadId),
                new MultipartSession(uploadId, userKey, bucket, tenant.tenantId()));

        ctx.contentType("application/xml");
        ctx.result("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<InitiateMultipartUploadResult>"
                + "<Bucket>" + xmlEscape(bucket) + "</Bucket>"
                + "<Key>" + xmlEscape(userKey) + "</Key>"
                + "<UploadId>" + xmlEscape(uploadId) + "</UploadId>"
                + "</InitiateMultipartUploadResult>");
    }

    /** Looks up an in-progress multipart session, rejecting (404 NoSuchUpload)
     *  if it's missing or was initiated on a different bucket/key than the
     *  request path. S3 scopes an uploadId to the (bucket, key) it was created
     *  on, so a valid uploadId under a different path is treated as nonexistent. */
    private MultipartSession lookupSession(Context ctx, TenantCtx tenant, String uploadId) {
        MultipartSession session = sessions.get(tenant.sessionKey(uploadId));
        if (session == null
                || !session.bucket.equals(ctx.pathParam("bucket"))
                || !session.key.equals(ctx.pathParam("key"))) {
            writeS3Error(ctx, 404, "NoSuchUpload", "The specified upload does not exist.");
            return null;
        }
        return session;
    }

    private void uploadPart(Context ctx, String uploadId) {
        TenantCtx tenant = resolver.resolve(ctx);
        if (tenant == null) return;
        MultipartSession session = lookupSession(ctx, tenant, uploadId);
        if (session == null) return;
        String partNumberParam = ctx.queryParam("partNumber");
        if (partNumberParam == null) {
            writeS3Error(ctx, 400, "InvalidArgument",
                    "partNumber query parameter is required for uploadPart.");
            return;
        }
        int partNumber;
        try {
            partNumber = Integer.parseInt(partNumberParam);
        } catch (NumberFormatException e) {
            writeS3Error(ctx, 400, "InvalidArgument",
                    "partNumber must be a valid integer, got: " + partNumberParam);
            return;
        }
        ResolvedBody rb = resolveBody(ctx, 400, "InvalidArgument",
                "Content-Length header is required for UploadPart.");
        if (rb == null) return;
        if (rb.length() > MAX_PART_BYTES) {
            writeS3Error(ctx, 400, "EntityTooLarge",
                    "Part size exceeds MAX_PART_BYTES (" + MAX_PART_BYTES + ").");
            return;
        }
        byte[] body;
        try (var stream = rb.stream()) {
            body = stream.readAllBytes();
        } catch (java.io.IOException e) {
            writeS3Error(ctx, 400, "InvalidArgument",
                    "Failed to read request body: " + e.getMessage());
            return;
        }
        if (body.length > MAX_PART_BYTES) {
            writeS3Error(ctx, 400, "EntityTooLarge",
                    "Part size exceeds MAX_PART_BYTES (" + MAX_PART_BYTES + ").");
            return;
        }

        session.lock.lock();
        try {
            if (partNumber != session.nextExpectedPartNumber) {
                writeS3Error(ctx, 400, "InvalidPart",
                        "Expected partNumber=" + session.nextExpectedPartNumber
                                + ", got " + partNumber + ". Parts must be sequential.");
                return;
            }

            if (session.pendingPartBytes != null) {
                // The previous pending part is about to be flushed as a
                // non-last part. The S3 Encryption Client requires non-last
                // parts to be a multiple of the AES block size (16 bytes).
                if (session.pendingPartBytes.length % 16 != 0) {
                    writeS3Error(ctx, 400, "InvalidArgument",
                            "Previous part (partNumber=" + session.pendingPartNumber
                                    + ") was " + session.pendingPartBytes.length
                                    + " bytes, which is not a multiple of 16. "
                                    + "Only the final multipart part may have an unaligned size. "
                                    + "Pad part " + session.pendingPartNumber
                                    + " to a multiple of 16 bytes, or finish the upload "
                                    + "with CompleteMultipartUpload (treating it as the last part).");
                    return;
                }
                CompletedPart cp = flushPart(tenant, session, false);
                session.completedParts.add(cp);
            }

            String etag = "\"" + md5Hex(body) + "\"";
            session.pendingPartNumber = partNumber;
            session.pendingPartBytes = body;
            session.pendingPartEtag = etag;
            session.nextExpectedPartNumber = partNumber + 1;
            ctx.header("ETag", etag);
        } finally {
            session.lock.unlock();
        }

        ctx.status(HttpStatus.OK);
    }

    private void completeMultipartUpload(Context ctx, String uploadId) {
        TenantCtx tenant = resolver.resolve(ctx);
        if (tenant == null) return;
        MultipartSession session = lookupSession(ctx, tenant, uploadId);
        if (session == null) return;

        CompleteMultipartUploadResponse resp;
        session.lock.lock();
        try {
            if (session.pendingPartBytes != null) {
                CompletedPart cp = flushPart(tenant, session, true);
                session.completedParts.add(cp);
            }

            CompleteMultipartUploadRequest req = CompleteMultipartUploadRequest.builder()
                    .bucket(session.bucket).key(tenant.s3Key(session.key)).uploadId(uploadId)
                    .multipartUpload(b -> b.parts(session.completedParts))
                    .build();
            resp = tenant.client().completeMultipartUpload(req);
        } finally {
            session.lock.unlock();
        }
        sessions.remove(tenant.sessionKey(uploadId));

        ctx.contentType("application/xml");
        ctx.result("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<CompleteMultipartUploadResult>"
                + "<Location>http://" + xmlEscape(session.bucket) + "/" + xmlEscape(session.key) + "</Location>"
                + "<Bucket>" + xmlEscape(session.bucket) + "</Bucket>"
                + "<Key>" + xmlEscape(session.key) + "</Key>"
                + "<ETag>" + xmlEscape(resp.eTag() != null ? resp.eTag() : "") + "</ETag>"
                + "</CompleteMultipartUploadResult>");
    }

    private void abortMultipartUpload(Context ctx, String uploadId) {
        TenantCtx tenant = resolver.resolve(ctx);
        if (tenant == null) return;
        MultipartSession session = lookupSession(ctx, tenant, uploadId);
        if (session == null) return;
        session.lock.lock();
        try {
            tenant.client().abortMultipartUpload(AbortMultipartUploadRequest.builder()
                    .bucket(session.bucket).key(tenant.s3Key(session.key)).uploadId(uploadId).build());
            sessions.remove(tenant.sessionKey(uploadId));
        } finally {
            session.lock.unlock();
        }
        ctx.status(HttpStatus.NO_CONTENT);
    }

    private CompletedPart flushPart(TenantCtx tenant, MultipartSession session, boolean isLast) {
        UploadPartRequest.Builder b = UploadPartRequest.builder()
                .bucket(session.bucket).key(tenant.s3Key(session.key))
                .uploadId(session.uploadId)
                .partNumber(session.pendingPartNumber);
        if (isLast) {
            b.sdkPartType(SdkPartType.LAST);
        }
        UploadPartResponse resp = tenant.client().uploadPart(b.build(),
                RequestBody.fromBytes(session.pendingPartBytes));
        CompletedPart cp = CompletedPart.builder()
                .partNumber(session.pendingPartNumber)
                .eTag(resp.eTag())
                .build();
        session.pendingPartBytes = null;
        session.pendingPartNumber = -1;
        session.pendingPartEtag = null;
        return cp;
    }

    // --- Helpers -------------------------------------------------------------

    private static void handleS3Exception(S3Exception e, Context ctx) {
        int status = e.statusCode() > 0 ? e.statusCode() : 500;
        String code = e.awsErrorDetails() != null && e.awsErrorDetails().errorCode() != null
                ? e.awsErrorDetails().errorCode()
                : "InternalError";
        String message = e.awsErrorDetails() != null && e.awsErrorDetails().errorMessage() != null
                ? e.awsErrorDetails().errorMessage()
                : e.getMessage();
        if (ctx.method() == HandlerType.HEAD) {
            ctx.status(status);
            return;
        }
        writeS3Error(ctx, status, code, message);
    }

    /** Package-private so resolver impls can use it for header-validation errors. */
    static void writeS3Error(Context ctx, int status, String code, String message) {
        ctx.status(status);
        ctx.contentType("application/xml");
        ctx.result("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<Error><Code>" + xmlEscape(code) + "</Code>"
                + "<Message>" + xmlEscape(message) + "</Message></Error>");
    }

    private static String xmlEscape(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    /** Inbound body paired with its declared plaintext length. {@code stream}
     *  is either the raw request body or an {@link AwsChunkedInputStream}
     *  wrapping it (for Content-Encoding: aws-chunked uploads). */
    private record ResolvedBody(java.io.InputStream stream, long length) {}

    /** Resolves the inbound body to (stream, plaintext-length), transparently
     *  handling Content-Encoding: aws-chunked (the streaming PUT format every
     *  modern AWS SDK uses by default). Writes a 4xx error to {@code ctx} and
     *  returns {@code null} on validation failure — callers bail on null.
     *
     *  The {@code missingLengthStatus/Code/Message} parameters control the
     *  error emitted on the non-chunked path when Content-Length is missing;
     *  PutObject uses 411 / MissingContentLength to match S3, UploadPart uses
     *  400 / InvalidArgument.
     */
    private ResolvedBody resolveBody(Context ctx, int missingLengthStatus,
                                     String missingLengthCode, String missingLengthMessage) {
        String enc = ctx.header("Content-Encoding");
        if (enc != null && enc.toLowerCase(java.util.Locale.ROOT).contains("aws-chunked")) {
            String decoded = ctx.header("X-Amz-Decoded-Content-Length");
            if (decoded == null) {
                writeS3Error(ctx, 400, "InvalidArgument",
                        "X-Amz-Decoded-Content-Length header is required for aws-chunked uploads.");
                return null;
            }
            long length;
            try {
                length = Long.parseLong(decoded);
            } catch (NumberFormatException e) {
                writeS3Error(ctx, 400, "InvalidArgument",
                        "X-Amz-Decoded-Content-Length must be a valid integer, got: " + decoded);
                return null;
            }
            return new ResolvedBody(new AwsChunkedInputStream(ctx.bodyInputStream()), length);
        }
        String lenHeader = ctx.header("Content-Length");
        if (lenHeader == null) {
            writeS3Error(ctx, missingLengthStatus, missingLengthCode, missingLengthMessage);
            return null;
        }
        long length;
        try {
            length = Long.parseLong(lenHeader);
        } catch (NumberFormatException e) {
            writeS3Error(ctx, 400, "InvalidArgument",
                    "Content-Length must be a valid integer, got: " + lenHeader);
            return null;
        }
        return new ResolvedBody(ctx.bodyInputStream(), length);
    }

    private static Map<String, String> extractUserMetadata(Context ctx) {
        Map<String, String> meta = new HashMap<>();
        for (Map.Entry<String, String> h : ctx.headerMap().entrySet()) {
            String name = h.getKey().toLowerCase();
            if (name.startsWith("x-amz-meta-")) {
                meta.put(name.substring("x-amz-meta-".length()), h.getValue());
            }
        }
        return meta;
    }

    private static void writeUserMetadataHeaders(Context ctx, Map<String, String> metadata) {
        if (metadata == null) return;
        for (Map.Entry<String, String> e : metadata.entrySet()) {
            // The encryption client stores its own metadata under x-amz-* keys
            // (e.g. x-amz-d, x-amz-i, x-amz-w). User metadata uses any other name.
            if (!e.getKey().toLowerCase().startsWith("x-amz-")) {
                ctx.header("x-amz-meta-" + e.getKey(), e.getValue());
            }
        }
    }

    /**
     * Walk the cause chain looking for the GCM auth-tag failure that AES-GCM
     * surfaces as javax.crypto.AEADBadTagException (subclass of BadPaddingException).
     * The S3 Encryption Client wraps this in its own exception.
     */
    private static boolean isAeadAuthFailure(Throwable t) {
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            if (cur instanceof BadPaddingException) return true;
            if (cur == cur.getCause()) break;
        }
        return false;
    }

    private static String md5Hex(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(data);
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
