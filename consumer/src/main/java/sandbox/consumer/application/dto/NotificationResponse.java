package sandbox.consumer.application.dto;

import java.time.Instant;
import java.util.UUID;

public record NotificationResponse(UUID id, String title, String text, Instant createdAt) {
}
