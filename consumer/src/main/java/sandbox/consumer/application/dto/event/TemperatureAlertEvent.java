package sandbox.consumer.application.dto.event;

import java.time.Instant;
import java.util.UUID;

public record TemperatureAlertEvent(
        UUID componentId,
        String text,
        Instant timestamp
) {
}
