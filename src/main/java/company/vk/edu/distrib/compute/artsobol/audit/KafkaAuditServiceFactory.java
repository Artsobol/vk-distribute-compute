package company.vk.edu.distrib.compute.artsobol.audit;

import company.vk.edu.distrib.compute.AuditService;
import company.vk.edu.distrib.compute.AuditServiceFactory;

import java.io.IOException;

public class KafkaAuditServiceFactory extends AuditServiceFactory {

    @Override
    protected AuditService doCreate(String bootstrapServers, String consumerGroupId) throws IOException {
        return new KafkaAuditService(bootstrapServers, consumerGroupId);
    }
}
