package company.vk.edu.distrib.compute.artsobol.audit;

import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaAuditService implements AuditService {
    private static final Logger log = LoggerFactory.getLogger(KafkaAuditService.class);
    private static final String AUDIT_TOPIC_NAME = "audit";
    private static final String AUDIT_STORAGE_DIR = "storage/audit";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    private static final int START_TIMEOUT_SECONDS = 5;
    private static final int STOP_TIMEOUT_MILLIS = 5_000;

    private final String bootstrapServers;
    private final String consumerGroupId;
    private final Path auditLogPath;
    private final List<AuditEvent> auditEntries = new CopyOnWriteArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private Thread consumerThread;

    KafkaAuditService(String bootstrapServers, String consumerGroupId) throws IOException {
        this.bootstrapServers = bootstrapServers;
        this.consumerGroupId = consumerGroupId;
        Path storageDir = Path.of(AUDIT_STORAGE_DIR);
        Files.createDirectories(storageDir);
        auditLogPath = Files.createTempFile(storageDir, sanitize(consumerGroupId) + "-", ".log");
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        CountDownLatch subscribed = new CountDownLatch(1);
        consumerThread = new Thread(() -> consume(subscribed), "audit-consumer-" + consumerGroupId);
        consumerThread.setDaemon(true);
        consumerThread.start();
        awaitSubscription(subscribed);
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        joinConsumerThread();
    }

    @Override
    public List<AuditEvent> listAuditEntries() {
        return List.copyOf(auditEntries);
    }

    private void consume(CountDownLatch subscribed) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties());
        try {
            consumer.subscribe(List.of(AUDIT_TOPIC_NAME), new AuditRebalanceListener(subscribed));
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                if (!records.isEmpty()) {
                    save(records);
                    consumer.commitSync();
                }
            }
        } catch (RuntimeException exception) {
            log.error("Audit consumer stopped because of an error", exception);
        } finally {
            closeConsumer(consumer);
            running.set(false);
            subscribed.countDown();
        }
    }

    private void save(ConsumerRecords<String, String> records) {
        for (ConsumerRecord<String, String> record : records) {
            AuditEvent event = AuditEventUtils.decode(record.value());
            append(event);
            auditEntries.add(event);
        }
    }

    private void append(AuditEvent event) {
        try {
            Files.writeString(
                    auditLogPath,
                    AuditEventUtils.encode(event) + System.lineSeparator(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.APPEND
            );
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to write audit event", exception);
        }
    }

    private Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return properties;
    }

    private static void closeConsumer(KafkaConsumer<String, String> consumer) {
        try {
            consumer.close();
        } catch (RuntimeException exception) {
            log.warn("Failed to close audit consumer", exception);
        }
    }

    private static void awaitSubscription(CountDownLatch subscribed) {
        try {
            boolean subscriptionCompleted = subscribed.await(START_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!subscriptionCompleted) {
                log.warn("Audit consumer subscription did not complete within {} seconds", START_TIMEOUT_SECONDS);
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private void joinConsumerThread() {
        Thread thread = consumerThread;
        if (thread == null) {
            return;
        }
        try {
            thread.join(STOP_TIMEOUT_MILLIS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private static String sanitize(String value) {
        return value.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    private record AuditRebalanceListener(CountDownLatch subscribed) implements ConsumerRebalanceListener {

        @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // Offsets are committed after each stored batch.
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                subscribed.countDown();
            }
        }
}
