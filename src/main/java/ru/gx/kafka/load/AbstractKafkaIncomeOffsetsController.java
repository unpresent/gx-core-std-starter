package ru.gx.kafka.load;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import ru.gx.kafka.PartitionOffset;
import ru.gx.kafka.entities.KafkaIncomeOffsetEntity;
import ru.gx.kafka.repository.KafkaOffsetsRepository;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.stream.Collectors;

import static lombok.AccessLevel.*;

@Slf4j
public abstract class AbstractKafkaIncomeOffsetsController implements KafkaIncomeOffsetsController {
    @Getter
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    private IncomeTopicsConfiguration incomeTopicsConfiguration;

    @Getter
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    private KafkaOffsetsRepository kafkaOffsetsRepository;

    @SuppressWarnings("unused")
    @Override
    public void seekIncomeOffsetsOnStart() {
        final var offsets = this.kafkaOffsetsRepository.findAll();

        var partitionsOffset = new ArrayList<PartitionOffset>();
        for (var topicDescriptor : this.incomeTopicsConfiguration.getAll()) {
            partitionsOffset.clear();
            offsets.forEach(o -> {
                if (o.getTopic() != null && o.getTopic().equals(topicDescriptor.getTopic())) {
                    partitionsOffset.add(new PartitionOffset(o.getPartition(), o.getOffset()));
                }
            });
            if (partitionsOffset.size() <= 0) {
                log.info("Topic: {}. Seek to begin all partitions in topic", topicDescriptor.getTopic());
                this.incomeTopicsConfiguration.seekTopicAllPartitionsToBegin(topicDescriptor.getTopic());
            } else {
                log.info("Topic: {}. Seek to {}", partitionsOffset.stream().map(po -> po.getPartition() + ":" + po.getOffset()).collect(Collectors.joining(",")), topicDescriptor.getTopic());
                this.incomeTopicsConfiguration.seekTopic(topicDescriptor.getTopic(), partitionsOffset);
            }
        }
    }

    /**
     * Сохранение в БД текущих смещений Kafka.
     */
    @SuppressWarnings("unused")
    @Override
    public void saveKafkaOffsets() {
        final var kafkaOffsets = new ArrayList<KafkaIncomeOffsetEntity>();
        final var pCount = this.incomeTopicsConfiguration.prioritiesCount();
        for (int p = 0; p < pCount; p++) {
            final var descriptorsByPriority =
                    this.incomeTopicsConfiguration.getByPriority(p);
            if (descriptorsByPriority == null) {
                throw new InvalidParameterException("Can't get descriptors by priority " + p);
            }
            descriptorsByPriority
                    .forEach(topicDescriptor ->
                            topicDescriptor.getPartitions().forEach(partition -> {
                                final var offset = new KafkaIncomeOffsetEntity()
                                        .setReader(this.incomeTopicsConfiguration.getReaderName())
                                        .setTopic(topicDescriptor.getTopic())
                                        .setPartition(partition)
                                        .setOffset(topicDescriptor.getOffset(partition));
                                kafkaOffsets.add(offset);
                            })
                    );
        }

        final var started = System.currentTimeMillis();
        this.kafkaOffsetsRepository.saveAllAndFlush(kafkaOffsets);
        log.info("KafkaOffsets: saved {} rows in {} ms", kafkaOffsets.size(), System.currentTimeMillis() - started);
    }
}
