package ru.gx.std.entities;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import ru.gx.data.jpa.AbstractEntityObject;

import javax.persistence.*;

@Entity
@IdClass(KafkaSnapshotPublishedOffsetEntityId.class)
@Table(schema = "Kafka", name = "SnapshotPublishedOffsets")
@Getter
@Setter
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = true)
@ToString
public class KafkaSnapshotPublishedOffsetEntity extends AbstractEntityObject {
    @Id
    @Column(name = "Topic", length = 100, nullable = false)
    private String topic;

    @Column(name = "Partition", nullable = false)
    private int partition;

    @Column(name = "StartOffset", nullable = false)
    private long startOffset;
}
