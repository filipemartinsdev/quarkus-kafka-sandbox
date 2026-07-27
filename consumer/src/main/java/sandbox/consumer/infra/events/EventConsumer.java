package sandbox.consumer.infra.events;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import sandbox.consumer.application.dto.event.TemperatureAlertEvent;
import sandbox.consumer.application.service.TelemetryService;

@ApplicationScoped
public class EventConsumer {

    private final TelemetryService telemetryService;

    @Inject
    public EventConsumer(TelemetryService telemetryService) {
        this.telemetryService = telemetryService;
    }

    @Incoming("temperature-alerts")
    public void consumeTemperatureAlert(TemperatureAlertEvent event) {
        telemetryService.handleTemperatureAlertEvent(event);
    }
}
