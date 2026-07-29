package sandbox.consumer.application.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sandbox.consumer.application.dto.event.TemperatureAlertEvent;
import sandbox.consumer.infra.cache.TemperatureAlertCacheStorage;

@ApplicationScoped
public class TelemetryService {
    private static final Logger log = LoggerFactory.getLogger(TelemetryService.class);
    private final TemperatureAlertCacheStorage temperatureAlertCacheStorage;
    private final NotificationService notificationService;

    @Inject
    public TelemetryService(TemperatureAlertCacheStorage temperatureAlertCacheStorage, NotificationService notificationService) {
        this.temperatureAlertCacheStorage = temperatureAlertCacheStorage;
        this.notificationService = notificationService;
    }

    public void handleTemperatureAlertEvent(TemperatureAlertEvent event){
        if (hasRecentlyNotified(event))
            return;

        notificationService.create(event);
        temperatureAlertCacheStorage.put(event);
    }

    private boolean hasRecentlyNotified(TemperatureAlertEvent event){
        return temperatureAlertCacheStorage.get(event.componentId()) != null;
    }
}
