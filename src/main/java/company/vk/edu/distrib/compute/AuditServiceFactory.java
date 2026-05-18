package company.vk.edu.distrib.compute;

import java.io.IOException;

public abstract class AuditServiceFactory {

    public AuditService create(String bootstrapServers, String consumerGroupId) throws IOException {
        return doCreate(bootstrapServers, consumerGroupId);
    }

    protected abstract AuditService doCreate(String bootstrapServers, String consumerGroupId) throws IOException;
}
