package com.tinfoil;

import io.javalin.Javalin;
import software.amazon.awssdk.services.s3.S3Client;

public class Main {
    public static void main(String[] args) {
        Config config = Config.load();

        // Pick a resolver strategy. Single-tenant holds one pre-built encryption
        // client; multitenant holds a per-key LRU and parses headers per request.
        TenantResolver resolver = config.multitenant()
                ? new MultiTenantResolver(new TenantClients(config))
                : new SingleTenantResolver(S3Clients.encrypted(config));

        // Non-encryption S3 client used for housekeeping ops
        S3Client housekeepingClient = S3Client.builder()
                .region(config.region())
                .credentialsProvider(config.creds())
                .build();

        Javalin app = Javalin.create(cfg -> {
            cfg.http.maxRequestSize = S3Routes.MAX_PART_BYTES;
        });
        S3Routes routes = new S3Routes(resolver, housekeepingClient, config);
        routes.register(app);
        app.start(config.port());

        System.out.println("tinfoil-buckets-sidecar listening on :" + config.port()
                + (config.multitenant() ? " [multitenant]" : ""));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            app.stop();
            routes.shutdown();
            try { resolver.close(); } catch (Exception ignored) {}
            housekeepingClient.close();
        }));
    }
}
