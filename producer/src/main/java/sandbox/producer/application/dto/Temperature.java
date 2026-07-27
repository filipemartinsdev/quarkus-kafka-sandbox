package sandbox.producer.application.dto;

import java.time.Instant;
import java.util.UUID;

public record Temperature(
        UUID componentId,
        Float value,
        Instant timestamp
) {
}
