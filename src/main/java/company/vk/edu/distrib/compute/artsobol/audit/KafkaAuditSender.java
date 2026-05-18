package company.vk.edu.distrib.compute.artsobol.audit;

import company.vk.edu.distrib.compute.AuditEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class KafkaAuditSender {
    private static final Logger log = LoggerFactory.getLogger(KafkaAuditSender.class);
    private static final String AUDIT_TOPIC_NAME = "audit";

    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicBoolean asyncAudit = new AtomicBoolean();
    private String bootstrapServers;
    private Producer<String, String> auditProducer;

    public void setBootstrapServers(String bootstrapServers) {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("Bootstrap servers must not be blank");
        }

        lock.lock();
        try {
            this.bootstrapServers = bootstrapServers;
            closeProducer();
        } finally {
            lock.unlock();
        }
    }

    public void setAsync(boolean enabled) {
        asyncAudit.set(enabled);
    }

    public void send(String method, String id, long requestTimestamp) throws IOException {
        Producer<String, String> producer = producer();
        if (producer == null) {
            return;
        }

        AuditEvent event = new AuditEvent(method, id, requestTimestamp);
        ProducerRecord<String, String> record = new ProducerRecord<>(
                AUDIT_TOPIC_NAME,
                id,
                AuditEventUtils.encode(event)
        );

        if (asyncAudit.get()) {
            producer.send(record, (ignoredMetadata, exception) -> {
                if (exception != null) {
                    log.warn("Failed to send audit event asynchronously: method={}, id={}", method, id, exception);
                }
            });
            return;
        }

        try {
            producer.send(record).get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while sending audit event", exception);
        } catch (ExecutionException exception) {
            throw new IOException("Failed to send audit event", exception);
        }
    }

    public void close() {
        lock.lock();
        try {
            closeProducer();
        } finally {
            lock.unlock();
        }
    }

    private Producer<String, String> producer() {
        lock.lock();
        try {
            if (auditProducer != null) {
                return auditProducer;
            }
            if (bootstrapServers == null) {
                return null;
            }
            auditProducer = new KafkaProducer<>(producerProperties());
            return auditProducer;
        } finally {
            lock.unlock();
        }
    }

    private Properties producerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    private void closeProducer() {
        if (auditProducer != null) {
            auditProducer.close(Duration.ofSeconds(5));
            auditProducer = null;
        }
    }
}
