package sandbox.consumer.infra.cache;

import io.quarkus.cache.*;
import jakarta.enterprise.context.ApplicationScoped;
import sandbox.consumer.application.dto.event.TemperatureAlertEvent;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@ApplicationScoped
public class TemperatureAlertCacheStorage {
    private final Cache cache;

    public TemperatureAlertCacheStorage(@CacheName("temperature-alert") Cache cache) {
        this.cache = cache;
    }

    @CacheResult(cacheName = "temperature-alert")
    public TemperatureAlertEvent get(@CacheKey UUID componentId){
        return null;
    }

    @CacheResult(cacheName = "temperature-alert")
    public TemperatureAlertEvent put(TemperatureAlertEvent event){
        cache.as(CaffeineCache.class)
                .put(event.componentId(), CompletableFuture.completedFuture(event));
        return event;
    }
}
