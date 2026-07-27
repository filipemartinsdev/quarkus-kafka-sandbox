package sandbox.consumer.application.service;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;
import sandbox.consumer.application.dto.event.TemperatureAlertEvent;

public class TemperatureAlertDeserializer extends ObjectMapperDeserializer<TemperatureAlertEvent> {
    public TemperatureAlertDeserializer() {
        super(TemperatureAlertEvent.class);
    }
}
