package sandbox.consumer.application.service;

import jakarta.enterprise.context.ApplicationScoped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sandbox.consumer.application.dto.event.TemperatureAlertEvent;

@ApplicationScoped
public class TelemetryService {
    private static final Logger log = LoggerFactory.getLogger(TelemetryService.class);

    public void handleTemperatureAlertEvent(TemperatureAlertEvent event){
        log.info("Received event: {}", event);
    }
}
