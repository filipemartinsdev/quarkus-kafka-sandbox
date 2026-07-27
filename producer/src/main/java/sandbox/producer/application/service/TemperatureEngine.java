package sandbox.producer.application.service;

import jakarta.enterprise.context.ApplicationScoped;
import sandbox.producer.application.dto.Temperature;

import java.time.Instant;
import java.util.Random;
import java.util.UUID;

@ApplicationScoped
public class TemperatureEngine {
    private final UUID COMPONENT_ID = UUID.randomUUID();
    private final float MIN_TEMP = 18;
    private final float MAX_TEMP = 42;

    private final Random random = new Random();

    private float last = 18;

    public Temperature next(){
        int change = random.nextInt(-1,2); // get -1, 0 or 1
        float current = calcNext(change);

        return new Temperature(
                COMPONENT_ID,
                current,
                Instant.now()
        );
    }

    private float calcNext(int change) {
        if(change == -1) {
            if (last == MIN_TEMP)
                return last;
            return --last;
        }
        if(change == 0)
            return last;
        if(change == 1) {
            if (last == MAX_TEMP)
                return last;
            return ++last;
        }
        return last;
    }
}
