package ru.gx.std.entities;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@EqualsAndHashCode(callSuper = false)
@ToString
public class KafkaIncomeOffsetEntityId implements Serializable {
    private String reader;
    private String topic;
    private int partition;
}
