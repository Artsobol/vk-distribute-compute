package company.vk.edu.distrib.compute;

public interface AuditableKVService extends KVService {

    void setBootstrapServers(String bootstrapServers);

    void setAsync(boolean enabled);
}
