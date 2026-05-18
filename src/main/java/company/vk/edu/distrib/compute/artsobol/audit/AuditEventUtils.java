package company.vk.edu.distrib.compute.artsobol.audit;

import company.vk.edu.distrib.compute.AuditEvent;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public final class AuditEventUtils {
    private static final String SEPARATOR = "\t";
    private static final int METHOD_INDEX = 0;
    private static final int ID_INDEX = 1;
    private static final int TIMESTAMP_INDEX = 2;
    private static final int EVENT_PARTS = 3;

    private AuditEventUtils() {
    }

    public static String encode(AuditEvent event) {
        String encodedId = Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(event.id().getBytes(StandardCharsets.UTF_8));
        return event.method() + SEPARATOR + encodedId + SEPARATOR + event.timestamp();
    }

    public static AuditEvent decode(String encodedEvent) {
        String[] parts = encodedEvent.split(SEPARATOR, EVENT_PARTS);
        if (parts.length != EVENT_PARTS) {
            throw new IllegalArgumentException("Malformed audit event");
        }

        byte[] idBytes = Base64.getUrlDecoder().decode(parts[ID_INDEX]);
        String id = new String(idBytes, StandardCharsets.UTF_8);
        long timestamp = Long.parseLong(parts[TIMESTAMP_INDEX]);
        return new AuditEvent(parts[METHOD_INDEX], id, timestamp);
    }
}
