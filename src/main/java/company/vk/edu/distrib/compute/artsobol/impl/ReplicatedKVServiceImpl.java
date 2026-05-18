package company.vk.edu.distrib.compute.artsobol.impl;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.AuditableKVService;
import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.artsobol.audit.KafkaAuditSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.NoSuchElementException;

public class ReplicatedKVServiceImpl implements ReplicatedService, AuditableKVService {

    private static final Logger log = LoggerFactory.getLogger(ReplicatedKVServiceImpl.class);
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String STATUS_PATH = "/v0/status";
    private static final String REPLICA_STATS_PATH = "/stats/replica";
    private static final String ROOT_SUFFIX = "/";
    private static final String ACCESS_SUFFIX = "access";
    private static final int SINGLE_PATH_SEGMENT = 1;
    private static final int ACCESS_PATH_SEGMENTS = 2;

    private final HttpServer server;
    private final int serverPort;
    private final ReplicationCoordinator coordinator;
    private final KafkaAuditSender auditSender = new KafkaAuditSender();
    private boolean started;
    private boolean stopped;

    public ReplicatedKVServiceImpl(int serverPort, int replicaCount, Path storageRoot) {
        try {
            server = HttpServer.create();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create HTTP server", e);
        }
        this.serverPort = serverPort;
        this.coordinator = new ReplicationCoordinator(replicaCount, storageRoot);
        initServer();
    }

    @Override
    public void start() {
        if (started) {
            return;
        }
        if (stopped) {
            throw new IllegalStateException("Service has already been stopped");
        }
        if (log.isInfoEnabled()) {
            log.info("Replicated server starting on port: {}, replicas={}", serverPort, coordinator.replicaCount());
        }
        bindServer();
        server.start();
        started = true;
    }

    @Override
    public void stop() {
        if (!started) {
            coordinator.close();
            auditSender.close();
            stopped = true;
            return;
        }
        if (log.isInfoEnabled()) {
            log.info("Replicated server stopping");
        }
        server.stop(0);
        coordinator.close();
        auditSender.close();
        started = false;
        stopped = true;
    }

    @Override
    public int port() {
        return serverPort;
    }

    @Override
    public int numberOfReplicas() {
        return coordinator.replicaCount();
    }

    @Override
    public void disableReplica(int nodeId) {
        coordinator.disableReplica(nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        coordinator.enableReplica(nodeId);
    }

    @Override
    public void setBootstrapServers(String bootstrapServers) {
        auditSender.setBootstrapServers(bootstrapServers);
    }

    @Override
    public void setAsync(boolean enabled) {
        auditSender.setAsync(enabled);
    }

    private void initServer() {
        server.createContext(STATUS_PATH, new ErrorHttpHandler(this::handleStatus));
        server.createContext(ENTITY_PATH, new ErrorHttpHandler(this::handleEntity));
        server.createContext(REPLICA_STATS_PATH, new ErrorHttpHandler(this::handleReplicaStats));
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!METHOD_GET.equals(exchange.getRequestMethod())) {
            writeEmptyResponse(exchange, 405);
            return;
        }
        writeEmptyResponse(exchange, 200);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        long requestTimestamp = System.currentTimeMillis();
        EntityRequest request = EntityRequest.parse(exchange.getRequestURI().getRawQuery());
        auditSender.send(exchange.getRequestMethod(), request.id(), requestTimestamp);
        coordinator.validateAck(request.ack());

        switch (exchange.getRequestMethod()) {
            case METHOD_GET -> handleGet(exchange, request);
            case METHOD_PUT -> handlePut(exchange, request);
            case METHOD_DELETE -> handleDelete(exchange, request);
            default -> writeEmptyResponse(exchange, 405);
        }
    }

    private void handleGet(HttpExchange exchange, EntityRequest request) throws IOException {
        ReadResult result = coordinator.read(request.id(), request.ack());
        if (result.successfulResponses() < request.ack()) {
            logQuorumNotReached("Read", request.id(), request.ack(), result.successfulResponses());
            writeEmptyResponse(exchange, 500);
            return;
        }

        VersionedEntry entry = result.entry();
        if (entry == null || entry.tombstone()) {
            writeEmptyResponse(exchange, 404);
            return;
        }

        byte[] body = entry.body();
        if (body == null) {
            if (log.isErrorEnabled()) {
                log.error("Fresh entry without body: key={}", request.id());
            }
            writeEmptyResponse(exchange, 500);
            return;
        }

        writeResponse(exchange, body);
    }

    private void handlePut(HttpExchange exchange, EntityRequest request) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        int successfulWrites = coordinator.put(request.id(), body);
        if (successfulWrites < request.ack()) {
            logQuorumNotReached("Write", request.id(), request.ack(), successfulWrites);
            writeEmptyResponse(exchange, 500);
            return;
        }
        writeEmptyResponse(exchange, 201);
    }

    private void handleDelete(HttpExchange exchange, EntityRequest request) throws IOException {
        int successfulWrites = coordinator.delete(request.id());
        if (successfulWrites < request.ack()) {
            logQuorumNotReached("Delete", request.id(), request.ack(), successfulWrites);
            writeEmptyResponse(exchange, 500);
            return;
        }
        writeEmptyResponse(exchange, 202);
    }

    private void handleReplicaStats(HttpExchange exchange) throws IOException {
        if (!METHOD_GET.equals(exchange.getRequestMethod())) {
            writeEmptyResponse(exchange, 405);
            return;
        }

        String path = exchange.getRequestURI().getPath();
        String suffix = path.substring(REPLICA_STATS_PATH.length());
        if (suffix.isEmpty() || ROOT_SUFFIX.equals(suffix)) {
            throw new IllegalArgumentException("Missing replica id");
        }

        String[] parts = suffix.substring(1).split("/");
        int replicaId = parseReplicaId(parts[0]);

        if (parts.length == SINGLE_PATH_SEGMENT) {
            writeJsonResponse(exchange, coordinator.replicaStats(replicaId).toJson());
            return;
        }

        if (parts.length == ACCESS_PATH_SEGMENTS && ACCESS_SUFFIX.equals(parts[1])) {
            writeJsonResponse(exchange, coordinator.replicaAccessStats(replicaId).toJson());
            return;
        }

        writeEmptyResponse(exchange, 404);
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

    private void bindServer() {
        try {
            server.bind(new InetSocketAddress(serverPort), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to bind HTTP server to port " + serverPort, e);
        }
    }

    private static void logQuorumNotReached(String operation, String key, int ack, int successfulResponses) {
        if (log.isWarnEnabled()) {
            log.warn(
                    "{} quorum not reached: key={}, ack={}, successes={}",
                    operation,
                    key,
                    ack,
                    successfulResponses
            );
        }
    }

    private static void writeEmptyResponse(HttpExchange exchange, int status) throws IOException {
        exchange.sendResponseHeaders(status, -1);
    }

    private static void writeResponse(HttpExchange exchange, byte[] body) throws IOException {
        exchange.sendResponseHeaders(200, body.length);
        if (body.length > 0) {
            exchange.getResponseBody().write(body);
        }
    }

    private static void writeJsonResponse(HttpExchange exchange, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json; charset=UTF-8");
        writeResponse(exchange, bytes);
    }

    private record ErrorHttpHandler(HttpHandler delegate) implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try (exchange) {
                try {
                    delegate.handle(exchange);
                } catch (IllegalArgumentException e) {
                    writeEmptyResponse(exchange, 400);
                } catch (NoSuchElementException e) {
                    writeEmptyResponse(exchange, 404);
                } catch (IOException e) {
                    writeEmptyResponse(exchange, 500);
                } catch (RuntimeException e) {
                    if (log.isErrorEnabled()) {
                        log.error("Unexpected exception while handling request", e);
                    }
                    writeEmptyResponse(exchange, 500);
                }
            }
        }
    }
}
