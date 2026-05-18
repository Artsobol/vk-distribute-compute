package company.vk.edu.distrib.compute.artsobol.impl;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.AuditableKVService;
import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.artsobol.audit.KafkaAuditSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;

public class ReplicatedKVServiceImpl implements ReplicatedService, AuditableKVService {

    private static final Logger log = LoggerFactory.getLogger(ReplicatedKVServiceImpl.class);
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String STATUS_PATH = "/v0/status";
    private static final String REPLICA_STATS_PATH = "/stats/replica";

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
        server.createContext(STATUS_PATH, new ErrorHttpHandler(new StatusHttpHandler()));
        server.createContext(ENTITY_PATH, new ErrorHttpHandler(new EntityHttpHandler(coordinator, auditSender)));
        server.createContext(REPLICA_STATS_PATH, new ErrorHttpHandler(new ReplicaStatsHttpHandler(coordinator)));
    }

    private void bindServer() {
        try {
            server.bind(new InetSocketAddress(serverPort), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to bind HTTP server to port " + serverPort, e);
        }
    }

}
