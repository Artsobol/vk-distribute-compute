package company.vk.edu.distrib.compute.artsobol.impl;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.artsobol.audit.KafkaAuditSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

final class EntityHttpHandler implements HttpHandler {
    private static final Logger log = LoggerFactory.getLogger(EntityHttpHandler.class);
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private final ReplicationCoordinator coordinator;
    private final KafkaAuditSender auditSender;

    EntityHttpHandler(ReplicationCoordinator coordinator, KafkaAuditSender auditSender) {
        this.coordinator = coordinator;
        this.auditSender = auditSender;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        long requestTimestamp = System.currentTimeMillis();
        EntityRequest request = EntityRequest.parse(exchange.getRequestURI().getRawQuery());
        auditSender.send(exchange.getRequestMethod(), request.id(), requestTimestamp);
        coordinator.validateAck(request.ack());

        switch (exchange.getRequestMethod()) {
            case METHOD_GET -> handleGet(exchange, request);
            case METHOD_PUT -> handlePut(exchange, request);
            case METHOD_DELETE -> handleDelete(exchange, request);
            default -> HttpResponses.writeEmpty(exchange, 405);
        }
    }

    private void handleGet(HttpExchange exchange, EntityRequest request) throws IOException {
        ReadResult result = coordinator.read(request.id(), request.ack());
        if (result.successfulResponses() < request.ack()) {
            logQuorumNotReached("Read", request.id(), request.ack(), result.successfulResponses());
            HttpResponses.writeEmpty(exchange, 500);
            return;
        }

        VersionedEntry entry = result.entry();
        if (entry == null || entry.tombstone()) {
            HttpResponses.writeEmpty(exchange, 404);
            return;
        }

        byte[] body = entry.body();
        if (body == null) {
            if (log.isErrorEnabled()) {
                log.error("Fresh entry without body: key={}", request.id());
            }
            HttpResponses.writeEmpty(exchange, 500);
            return;
        }

        HttpResponses.write(exchange, body);
    }

    private void handlePut(HttpExchange exchange, EntityRequest request) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        int successfulWrites = coordinator.put(request.id(), body);
        if (successfulWrites < request.ack()) {
            logQuorumNotReached("Write", request.id(), request.ack(), successfulWrites);
            HttpResponses.writeEmpty(exchange, 500);
            return;
        }
        HttpResponses.writeEmpty(exchange, 201);
    }

    private void handleDelete(HttpExchange exchange, EntityRequest request) throws IOException {
        int successfulWrites = coordinator.delete(request.id());
        if (successfulWrites < request.ack()) {
            logQuorumNotReached("Delete", request.id(), request.ack(), successfulWrites);
            HttpResponses.writeEmpty(exchange, 500);
            return;
        }
        HttpResponses.writeEmpty(exchange, 202);
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
}
