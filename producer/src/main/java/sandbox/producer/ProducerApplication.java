package sandbox.producer;

import jakarta.enterprise.context.ApplicationScoped;
import io.quarkus.scheduler.Scheduled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sandbox.producer.application.dto.Temperature;
import sandbox.producer.application.dto.event.NewTemperatureEvent;
import sandbox.producer.application.service.TemperatureEngine;
import sandbox.producer.infra.events.EventProducer;

@ApplicationScoped
public class ProducerApplication {
    private static final Logger log = LoggerFactory.getLogger(ProducerApplication.class);
    private final TemperatureEngine temperatureEngine;
    private final EventProducer messageProducer;

    public ProducerApplication(TemperatureEngine temperatureEngine, EventProducer messageProducer) {
        this.temperatureEngine = temperatureEngine;
        this.messageProducer = messageProducer;
    }

    @Scheduled(cron = "*/2 * * * * ?")
    public void updateTemperature(){
        Temperature temperature = temperatureEngine.next();
        log.info("Current: {}°", temperature.value());

        messageProducer.produceNewTemperature(
                new NewTemperatureEvent(
                        temperature.componentId(),
                        temperature.value(),
                        temperature.timestamp()
                )
        );
    }
}
