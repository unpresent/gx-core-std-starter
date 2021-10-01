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
@IdClass(KafkaIncomeOffsetEntityId.class)
@Table(schema = "Kafka", name = "IncomeOffsets")
@Getter
@Setter
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = true)
@ToString
public class KafkaIncomeOffsetEntity extends AbstractEntityObject {
    @Id
    @Column(name = "Reader", length = 100, nullable = false)
    private String reader;

    @Id
    @Column(name = "Topic", length = 100, nullable = false)
    private String topic;

    @Id
    @Column(name = "Partition", nullable = false)
    private int partition;

    @Column(name = "Offset")
    private long offset;
}
