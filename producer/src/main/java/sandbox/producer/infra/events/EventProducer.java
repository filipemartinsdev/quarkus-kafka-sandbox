package sandbox.producer.infra.events;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import sandbox.producer.application.dto.event.NewTemperatureEvent;

@ApplicationScoped
public class EventProducer {
    @Inject
    @Channel("temperature-updated")
    Emitter<NewTemperatureEvent> emitter;

    public void produceNewTemperature(NewTemperatureEvent event) {
        emitter.send(event);
    }
}
