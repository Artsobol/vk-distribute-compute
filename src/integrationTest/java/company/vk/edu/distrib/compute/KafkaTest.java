package company.vk.edu.distrib.compute;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("PMD.TestClassWithoutTestCases")
@Disabled
@ParameterizedClass
@ArgumentsSource(AuditServiceFactoryArgumentsProvider.class)
@Testcontainers
public class KafkaTest extends TestBase {
    private static final String PUT = "PUT";
    private static final String GET = "GET";
    private static final String DELETE = "DELETE";
    private static final String AUDIT_TOPIC_NAME = "audit";

    static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    @Parameter(0)
    KVServiceFactory kvServiceFactory;

    @Parameter(1)
    AuditServiceFactory auditServiceFactory;

    @Container
    private static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.7.8")
    );

    private String bootstrapServers;
    private AdminClient adminClient;

    @Test
    void shouldAuditPutGetDelete() {
        assertTimeoutPreemptively(TIMEOUT, () -> {

            AuditService auditService = auditServiceFactory.create(bootstrapServers, "cg_t1_1");
            auditService.start();

            int port = randomPort();
            String endpoint = endpoint(port);
            KVService storage = kvServiceFactory.create(port);
            if (storage instanceof AuditableKVService auditableStorage) {
                auditableStorage.setBootstrapServers(bootstrapServers);
            }
            storage.start();

            try {
                String key = randomKey();
                byte[] value = randomValue();

                assertEquals(201, upsert(endpoint, key, value).statusCode());

                final HttpResponse<byte[]> response = get(endpoint, key);
                assertEquals(200, response.statusCode());
                assertArrayEquals(value, response.body());

                assertEquals(202, delete(endpoint, key).statusCode());

                Thread.sleep(500);

                var events = auditService.listAuditEntries().stream()
                        .filter(it -> key.equals(it.id()))
                        .toList();
                assertEquals(3, events.size());
                assertArrayEquals(
                        new String[] {PUT, GET, DELETE},
                        events.stream().map(AuditEvent::method).toArray()
                );
            } finally {
                auditService.stop();
                storage.stop();
            }
        });
    }

    @Test
    void shouldAuditNotFound() {
        assertTimeoutPreemptively(TIMEOUT, () -> {

            AuditService auditService = auditServiceFactory.create(bootstrapServers, "cg_t2_1");
            auditService.start();

            int port = randomPort();
            String endpoint = endpoint(port);
            KVService storage = kvServiceFactory.create(port);
            if (storage instanceof AuditableKVService auditableStorage) {
                auditableStorage.setBootstrapServers(bootstrapServers);
            }
            storage.start();

            try {
                String key = randomKey();

                final HttpResponse<byte[]> response = get(endpoint, key);
                assertEquals(404, response.statusCode());

                Thread.sleep(500);

                var events = auditService.listAuditEntries().stream()
                        .filter(it -> key.equals(it.id()))
                        .toList();
                assertEquals(1, events.size());
                assertArrayEquals(
                        new String[] {GET},
                        events.stream().map(AuditEvent::method).toArray()
                );

            } finally {
                auditService.stop();
                storage.stop();
            }
        });
    }

    @Test
    void shouldAuditSecondConsumerAddedInGroup() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            String consumerGroupId = "cg_t3_1";
            AuditService auditService1 = auditServiceFactory.create(bootstrapServers, consumerGroupId);
            auditService1.start();

            AuditService auditService2 = auditServiceFactory.create(bootstrapServers, consumerGroupId);

            int port = randomPort();
            String endpoint = endpoint(port);
            KVService storage = kvServiceFactory.create(port);
            if (storage instanceof AuditableKVService auditableStorage) {
                auditableStorage.setBootstrapServers(bootstrapServers);
            }
            storage.start();

            try {
                String key = randomKey();
                byte[] value = randomValue();
                upsert(endpoint, key, value).statusCode();

                Thread.sleep(500);

                var events1 = auditService1.listAuditEntries().stream()
                        .filter(it -> key.equals(it.id()))
                        .toList();

                assertEquals(1, events1.size());
                assertArrayEquals(
                        new String[] {PUT},
                        events1.stream().map(AuditEvent::method).toArray()
                );

                var events2 = auditService2.listAuditEntries().stream()
                        .filter(it -> key.equals(it.id()))
                        .toList();
                assertTrue(events2.isEmpty());

                auditService2.start();
                Thread.sleep(500);

                assertTrue(
                        auditService2.listAuditEntries().stream()
                                .filter(it -> key.equals(it.id()))
                                .toList()
                                .isEmpty()
                );

            } finally {
                auditService1.stop();
                auditService2.stop();
                storage.stop();
            }
        });
    }

    @Test
    void shouldAuditTwoConsumerGroups() {
        assertTimeoutPreemptively(TIMEOUT, () -> {

            AuditService auditService1 = auditServiceFactory.create(bootstrapServers, "cg_t4_1");
            auditService1.start();

            AuditService auditService2 = auditServiceFactory.create(bootstrapServers, "cg_t4_2");

            int port = randomPort();
            String endpoint = endpoint(port);
            KVService storage = kvServiceFactory.create(port);
            if (storage instanceof AuditableKVService auditableStorage) {
                auditableStorage.setBootstrapServers(bootstrapServers);
            }
            storage.start();

            try {
                String key = randomKey();
                byte[] value = randomValue();

                assertEquals(201, upsert(endpoint, key, value).statusCode());

                Thread.sleep(500);

                List<AuditEvent> auditEvents1 = auditService1.listAuditEntries();
                var events1 = auditEvents1.stream()
                        .filter(it -> key.equals(it.id()))
                        .toList();
                assertEquals(1, events1.size());
                assertArrayEquals(
                        new String[] {PUT},
                        events1.stream().map(AuditEvent::method).toArray()
                );

                assertTrue(
                        auditService2.listAuditEntries().stream()
                                .filter(it -> key.equals(it.id()))
                                .toList()
                                .isEmpty()
                );

                auditService2.start();
                Thread.sleep(500);

                List<AuditEvent> auditEvents2 = auditService1.listAuditEntries();
                var events2 = auditEvents2.stream()
                        .filter(it -> key.equals(it.id()))
                        .toList();
                assertEquals(1, events2.size());
                assertArrayEquals(
                        new String[] {PUT},
                        events2.stream().map(AuditEvent::method).toArray()
                );

            } finally {
                auditService1.stop();
                auditService2.stop();
                storage.stop();
            }
        });
    }

    static String getBootstrapServers() {
        return KAFKA.getBootstrapServers();
    }

    @AfterAll
    static void afterAll() {
        HTTP_CLIENT.close();
    }

    @BeforeEach
    void createTopic() throws ExecutionException, InterruptedException {
        Set<String> topics = adminClient.listTopics().names().get();
        if (!topics.contains(AUDIT_TOPIC_NAME)) {
            NewTopic auditTopic = new NewTopic(AUDIT_TOPIC_NAME, 2, (short) 1);
            adminClient.createTopics(List.of(auditTopic));
        }
        bootstrapServers = KAFKA.getBootstrapServers();

        Properties adminProps = new Properties();
        adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminClient = AdminClient.create(adminProps);
    }

    @AfterEach
    void deleteTopic() throws ExecutionException, InterruptedException {
        Set<String> topics = adminClient.listTopics().names().get();
        if (topics.contains(AUDIT_TOPIC_NAME)) {
            adminClient.deleteTopics(List.of(AUDIT_TOPIC_NAME));
        }
        adminClient.close();
    }

    @Override
    protected HttpClient getHttpClient() {
        return HTTP_CLIENT;
    }
}
