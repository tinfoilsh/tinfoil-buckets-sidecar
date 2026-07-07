package com.tinfoil;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import software.amazon.awssdk.services.s3.model.CompletedPart;

public class MultipartSession {
    public final String uploadId;
    public final String key;
    public final String bucket;
    public final String tenantId; // null in single-tenant mode
    public final Instant createdAt = Instant.now();
    public final ReentrantLock lock = new ReentrantLock();

    public int nextExpectedPartNumber = 1;
    public int pendingPartNumber = -1;
    public byte[] pendingPartBytes = null;
    public String pendingPartEtag = null;
    public final List<CompletedPart> completedParts = new ArrayList<>();

    public MultipartSession(String uploadId, String key, String bucket, String tenantId) {
        this.uploadId = uploadId;
        this.key = key;
        this.bucket = bucket;
        this.tenantId = tenantId;
    }
}
