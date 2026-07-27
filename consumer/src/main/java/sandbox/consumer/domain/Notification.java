package sandbox.consumer.domain;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.UUID;

@Entity
public class Notification extends PanacheEntityBase {
    @Id @GeneratedValue(strategy = GenerationType.UUID)
    public UUID id;

    @NotEmpty
    public String title;

    @NotEmpty
    public String text;

    public Instant timestamp;

    @NotNull
    @Column(name = "created_at")
    public Instant createdAt = Instant.now();
}
