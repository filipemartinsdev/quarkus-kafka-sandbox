package sandbox.producer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sandbox.producer.application.dto.event.NewTemperatureEvent;
import sandbox.producer.application.dto.event.TemperatureAlertEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@ApplicationScoped
public class KafkaConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    @ConfigProperty(name = "app.alerts.temperature-limit", defaultValue = "20.0")
    float TEMPERATURE_LIMIT;

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String KAFKA_BOOTSTRAP_SERVERS;

    @Inject
    ObjectMapper objectMapper;

    @Produces
    public Topology topology(){
//        Step 1: get builder
        var streamsBuilder = new StreamsBuilder();

//        Step 2: create Serde
        Serde<NewTemperatureEvent> eventSerde = new ObjectMapperSerde<>(NewTemperatureEvent.class, objectMapper);
        Serde<TemperatureAccumulator> accumSerde = new ObjectMapperSerde<>(TemperatureAccumulator.class, objectMapper);
        Serde<TemperatureAlertEvent> alertSerde = new ObjectMapperSerde<>(TemperatureAlertEvent.class, objectMapper);

//        Step 3: Stream entry
        KStream<String, NewTemperatureEvent> sourceStream = streamsBuilder.stream(
                "sensor.temperature.updated",
                Consumed.with(Serdes.String(), eventSerde)
        );


//        Step 4: Tumbling Window of 1 minute
        var windowSize = Duration.ofMinutes(1);
        var gracePeriod = Duration.ofSeconds(10);
        var tumblingWindow = TimeWindows.ofSizeAndGrace(windowSize, gracePeriod);

//        Step 5: Pipeline definition
        sourceStream
                .groupBy(
                        (key, event) -> event.componentId().toString(),
                        Grouped.with(Serdes.String(), eventSerde)
                )

                .windowedBy(tumblingWindow)

                .aggregate(
                        () -> new TemperatureAccumulator(0.0f, 0),
                        (key, event, accum) -> {
                                return new TemperatureAccumulator(accum.sum + event.value(), accum.count + 1);
                        },
                        Materialized.with(Serdes.String(), accumSerde)
                )

                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))

                .toStream()

                .filter((key, event) -> event.getAverage() > TEMPERATURE_LIMIT)

                .map((windowedKey, accum) -> {
                    String alertMessage = String.format(
                            "The sensor %s exceeded the temperature limit of %.2f°. Last 1 minute average: %.2f°",
                            windowedKey.key(), TEMPERATURE_LIMIT, accum.getAverage()
                    );

                    var alertEvent = new TemperatureAlertEvent(
                            UUID.fromString(windowedKey.key()),
                            alertMessage,
                            Instant.now()
                    );

                    return new KeyValue<>(windowedKey.key(), alertEvent);
                })

                .peek((key, event) -> log.info("Producing alert for component: {}", key))

                .to("sensor.temperature.alerts", Produced.with(Serdes.String(), alertSerde));

        return streamsBuilder.build();
    }


    private static record TemperatureAccumulator (float sum, long count){
        public float getAverage(){
            return count == 0 ? 0.0f : sum / count;
        }
    };


    public void onStart(@Observes StartupEvent event){
        createKafkaTopics();
    }

    private void createKafkaTopics(){
        var properties = Map.<String, Object>of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);

        try(AdminClient adminClient = AdminClient.create(properties)){
            adminClient.createTopics(List.of(
                    new NewTopic("sensor.temperature.updated", 3, (short) 1),
                    new NewTopic("sensor.temperature.alerts", 1, (short) 1)
            ));
            log.info("Kafka topics created successfully");
        } catch (Exception e){
            log.warn("Kafka topic creation interrupted");
        }
    }
}
