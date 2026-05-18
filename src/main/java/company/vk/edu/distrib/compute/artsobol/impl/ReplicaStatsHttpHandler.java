package company.vk.edu.distrib.compute.artsobol.impl;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;

final class ReplicaStatsHttpHandler implements HttpHandler {
    private static final String METHOD_GET = "GET";
    private static final String REPLICA_STATS_PATH = "/stats/replica";
    private static final String ROOT_SUFFIX = "/";
    private static final String ACCESS_SUFFIX = "access";
    private static final int SINGLE_PATH_SEGMENT = 1;
    private static final int ACCESS_PATH_SEGMENTS = 2;

    private final ReplicationCoordinator coordinator;

    ReplicaStatsHttpHandler(ReplicationCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!METHOD_GET.equals(exchange.getRequestMethod())) {
            HttpResponses.writeEmpty(exchange, 405);
            return;
        }

        String suffix = exchange.getRequestURI().getPath().substring(REPLICA_STATS_PATH.length());
        if (suffix.isEmpty() || ROOT_SUFFIX.equals(suffix)) {
            throw new IllegalArgumentException("Missing replica id");
        }

        String[] parts = suffix.substring(1).split("/");
        int replicaId = parseReplicaId(parts[0]);

        if (parts.length == SINGLE_PATH_SEGMENT) {
            HttpResponses.writeJson(exchange, coordinator.replicaStats(replicaId).toJson());
            return;
        }

        if (parts.length == ACCESS_PATH_SEGMENTS && ACCESS_SUFFIX.equals(parts[1])) {
            HttpResponses.writeJson(exchange, coordinator.replicaAccessStats(replicaId).toJson());
            return;
        }

        HttpResponses.writeEmpty(exchange, 404);
    }

    private static int parseReplicaId(String rawReplicaId) {
        if (rawReplicaId == null || rawReplicaId.isEmpty()) {
            throw new IllegalArgumentException("Missing replica id");
        }
        try {
            return Integer.parseInt(rawReplicaId);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Replica id must be numeric", e);
        }
    }
}
