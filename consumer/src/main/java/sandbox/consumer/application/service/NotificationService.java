package sandbox.consumer.application.service;

import io.github.responsekit.core.PagedResponse;
import io.github.responsekit.quarkus.PagedResponseFactory;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sandbox.consumer.application.dto.NotificationResponse;
import sandbox.consumer.application.dto.event.TemperatureAlertEvent;
import sandbox.consumer.domain.Notification;

@ApplicationScoped
public class NotificationService {
    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    public PagedResponse<NotificationResponse> getAll(int page, int size) {
        return PagedResponseFactory.fromQuery(
                Notification.findAll().page(page, size),
                this::toResponse
        );
    }

    @Transactional
    public void create(TemperatureAlertEvent event){
        log.info("Creating notification from event: {}", event);

        var notification = new Notification();
        notification.title = "[ALERT] Component exceeded temperature";
        notification.text = event.text();
        notification.timestamp = event.timestamp();
        notification.persist();
    }

    private NotificationResponse toResponse(Notification entity){
        return new NotificationResponse(entity.id, entity.title, entity.text, entity.createdAt);
    }
}
