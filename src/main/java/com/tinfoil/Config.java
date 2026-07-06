package com.tinfoil;

import java.util.Base64;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import io.github.cdimascio.dotenv.Dotenv;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;

public record Config(
        SecretKey aesKey,
        Region region,
        AwsCredentialsProvider creds,
        int port,
        boolean delayedAuth,
        long bufferSize,
        boolean multitenant) {

    public static Config load() {
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
        boolean multitenant = parseBool(dotenv.get("MULTITENANT"), false);
        // In multitenant mode the encryption key arrives per-request 
        String envKey = dotenv.get("ENCRYPTION_KEY");
        SecretKey aesKey;
        if (multitenant) {
            aesKey = null;
        } else {
            if (envKey == null || envKey.isEmpty()) {
                throw new IllegalStateException("Missing ENCRYPTION_KEY on startup (set it in .env, or enable MULTITENANT=true)");
            }
            aesKey = loadAesKey(envKey);
        }
        return new Config(
                aesKey,
                resolveRegion(dotenv),
                resolveCreds(dotenv),
                parsePort(dotenv.get("PORT")),
                parseBool(dotenv.get("DANGEROUS_DELAYED_AUTH"), false),
                parseBufferSize(dotenv.get("BUFFER_SIZE")),
                multitenant);
    }

    private static int parsePort(String s) {
        return (s == null || s.isEmpty()) ? 9000 : Integer.parseInt(s);
    }

    private static boolean parseBool(String s, boolean defaultValue) {
        if (s == null || s.isEmpty()) return defaultValue;
        return Boolean.parseBoolean(s);
    }

    private static long parseBufferSize(String s) {
        // Default 1 GiB. Used only when DELAYED_AUTH=false. The encryption
        // client buffers the full plaintext in JVM heap before responding,
        // verifying the GCM auth tag before any byte leaves the sidecar.
        // Size against -Xmx (which is itself a subset of enclave memory).
        // The encryption client supports up to 64 GiB.
        //
        // For larger objects, set DANGEROUS_DELAYED_AUTH=true.
        return (s == null || s.isEmpty()) ? 1024L * 1024L * 1024L : Long.parseLong(s);
    }

    private static SecretKey loadAesKey(String b64) {
        byte[] bytes = Base64.getDecoder().decode(b64);
        if (bytes.length != 32) {
            throw new IllegalArgumentException(
                    "ENCRYPTION_KEY must decode to 32 bytes (AES-256), got " + bytes.length);
        }
        return new SecretKeySpec(bytes, "AES");
    }

    private static Region resolveRegion(Dotenv dotenv) {
        String r = dotenv.get("AWS_REGION");
        if (r != null && !r.isEmpty()) return Region.of(r);
        return new DefaultAwsRegionProviderChain().getRegion();
    }

    private static AwsCredentialsProvider resolveCreds(Dotenv dotenv) {
        String key = dotenv.get("AWS_ACCESS_KEY_ID");
        String secret = dotenv.get("AWS_SECRET_ACCESS_KEY");
        if (key != null && secret != null) {
            String token = dotenv.get("AWS_SESSION_TOKEN");
            if (token != null && !token.isEmpty()) {
                return StaticCredentialsProvider.create(
                        AwsSessionCredentials.create(key, secret, token));
            }
            return StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(key, secret));
        }
        String profile = dotenv.get("AWS_PROFILE");
        if (profile != null && !profile.isEmpty()) {
            return ProfileCredentialsProvider.create(profile);
        }
        return DefaultCredentialsProvider.builder().build();
    }
}
