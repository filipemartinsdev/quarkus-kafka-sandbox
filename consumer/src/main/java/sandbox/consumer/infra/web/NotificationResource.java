package sandbox.consumer.infra.web;

import io.github.responsekit.core.PagedResponse;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.jboss.resteasy.reactive.RestQuery;
import org.jboss.resteasy.reactive.RestResponse;
import sandbox.consumer.application.dto.NotificationResponse;
import sandbox.consumer.application.service.NotificationService;

@Produces(MediaType.APPLICATION_JSON)
@Path("/api/notifications")
public class NotificationResource {
    private final NotificationService notificationService;

    public NotificationResource(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @GET
    public RestResponse<PagedResponse<NotificationResponse>> getAll(
            @RestQuery @DefaultValue("0") int page, @RestQuery @DefaultValue("20") int size
    ){
        return RestResponse.ok(
                notificationService.getAll(page, size)
        );
    }
}
