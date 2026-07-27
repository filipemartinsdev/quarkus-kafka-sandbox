package sandbox.producer.application.dto.event;

import java.time.Instant;
import java.util.UUID;

public record NewTemperatureEvent(
        UUID componentId,
        Float value,
        Instant timestamp
) {
}
