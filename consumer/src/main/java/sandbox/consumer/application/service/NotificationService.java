package sandbox.consumer.application.service;

import io.github.responsekit.core.PagedResponse;
import io.github.responsekit.quarkus.PagedResponseFactory;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sandbox.consumer.application.dto.NotificationResponse;
import sandbox.consumer.application.dto.event.TemperatureAlertEvent;
import sandbox.consumer.domain.Notification;
import sandbox.consumer.infra.cache.TemperatureAlertCacheStorage;

@ApplicationScoped
public class NotificationService {
    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);
    private final TemperatureAlertCacheStorage temperatureAlertCacheStorage;

    @Inject
    public NotificationService(TemperatureAlertCacheStorage temperatureAlertCacheStorage) {
        this.temperatureAlertCacheStorage = temperatureAlertCacheStorage;
    }

    public PagedResponse<NotificationResponse> getAll(int page, int size) {
        return PagedResponseFactory.fromQuery(
                Notification.findAll().page(page, size),
                this::toResponse
        );
    }

    public void create(TemperatureAlertEvent event){
        log.info("Creating notification from event: {}", event);
        temperatureAlertCacheStorage.put(event);
    }

    private NotificationResponse toResponse(Notification entity){
        return new NotificationResponse(entity.id, entity.title, entity.text, entity.createdAt);
    }
}
