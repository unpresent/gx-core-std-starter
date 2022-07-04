package ru.gx.core.std.offsets;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.gx.core.channels.ChannelDirection;
import ru.gx.core.channels.ChannelsConfiguration;
import ru.gx.core.kafka.KafkaConstants;
import ru.gx.core.kafka.load.KafkaIncomeTopicLoadingDescriptor;
import ru.gx.core.kafka.offsets.PartitionOffset;
import ru.gx.core.kafka.offsets.TopicPartitionOffset;
import ru.gx.core.kafka.offsets.TopicsOffsetsStorage;
import ru.gx.core.messaging.Message;

import java.util.*;

@Slf4j
@RequiredArgsConstructor
public class KafkaTopicsOffsetsStorage implements TopicsOffsetsStorage {

    @Override
    @Nullable
    public Collection<TopicPartitionOffset> loadOffsets(
            @NotNull final ChannelDirection direction,
            @NotNull final String serviceName,
            @NotNull final ChannelsConfiguration configuration
    ) {
        if (direction != ChannelDirection.In) {
            throw new UnsupportedOperationException(
                    "Unsupported direction " + direction + " for " + this.getClass().getName()
            );
        }
        final var result = new ArrayList<TopicPartitionOffset>();
        for (final var descriptor : configuration.getAll()) {
            if (!(descriptor instanceof final KafkaIncomeTopicLoadingDescriptor kafkaIncomeDescriptor)) {
                throw new UnsupportedOperationException(
                        "Unsupported configuration descriptor " + descriptor.getClass().getName()
                                + " for " + this.getClass().getName()
                );
            }
            final var topicPartitions = (Set<TopicPartition>) Set.copyOf(
                    kafkaIncomeDescriptor.getTopicPartitions()
            );
            final var consumer = kafkaIncomeDescriptor.getConsumer();
            Map<TopicPartition, OffsetAndMetadata> committed;
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (consumer) {
                committed = consumer.committed(topicPartitions);
            }

            committed
                    .forEach((key, value) -> result.add(
                            new TopicPartitionOffset(
                                    key.topic(),
                                    key.partition(),
                                    value == null ? 0 : value.offset()
                            ))
                    );

        }
        return result;
    }

    @Override
    public void saveOffsets(
            @NotNull final ChannelDirection direction,
            @NotNull final String readerName,
            @NotNull final ChannelsConfiguration configuration,
            @NotNull final Collection<TopicPartitionOffset> offsets
    ) {
        configuration.getAll().forEach(descriptor -> {
            // Пробегаемся по дескрипторам конфигурации
            final var kafkaDescriptor = (KafkaIncomeTopicLoadingDescriptor) descriptor;
            final var localOffsets = new ArrayList<PartitionOffset>();
            kafkaDescriptor.getTopicPartitions().forEach(topicPartition -> {
                // Пробегаемся по всем парам <Partition, Offset>
                offsets.forEach(offset -> {
                    if (offset.getTopic().equals(topicPartition.topic())
                            && offset.getPartition() == topicPartition.partition()) {
                        // Отбираем из пачки всех offsets (вх. параметр) только подходящие
                        localOffsets.add(new PartitionOffset(offset.getPartition(), offset.getOffset()));
                    }
                });
            });
            // И сохраняем отобранные
            internalSaveOffsets(kafkaDescriptor.getChannelName(), kafkaDescriptor.getConsumer(), localOffsets);
        });
    }

    @Override
    public void saveOffsetFromMessage(
            @NotNull final ChannelDirection channelDirection,
            @NotNull final String serviceName,
            @NotNull final Message<?> message
    ) {
        final var partition = (Integer) message.getMetadataValue(KafkaConstants.METADATA_PARTITION);
        if (partition == null) {
            throw new NullPointerException("Message doesn't have metadata " + KafkaConstants.METADATA_PARTITION + "!");
        }
        final var offset = (Long) message.getMetadataValue(KafkaConstants.METADATA_OFFSET);
        if (offset == null) {
            throw new NullPointerException("Message doesn't have metadata " + KafkaConstants.METADATA_OFFSET + "!");
        }
        final var topicName = message.getChannelDescriptor().getChannelName();

        if (!(message.getChannelDescriptor() instanceof final KafkaIncomeTopicLoadingDescriptor kafkaIncomeDescriptor)) {
            throw new UnsupportedOperationException(
                    "Unsupported configuration descriptor " + message.getChannelDescriptor().getClass().getName()
                            + " for " + this.getClass().getName()
            );
        }

        internalSaveOffsets(
                kafkaIncomeDescriptor.getChannelName(),
                kafkaIncomeDescriptor.getConsumer(),
                Set.of(new TopicPartitionOffset(topicName, partition, offset))
        );
    }

    protected void internalSaveOffsets(
            @NotNull final String topic,
            @NotNull final Consumer<?, ?> consumer,
            @NotNull final Iterable<PartitionOffset> offsets
    ) {
        final var map = new HashMap<TopicPartition, OffsetAndMetadata>();
        offsets.forEach(o ->
                map.put(
                        new TopicPartition(topic, o.getPartition()),
                        // Добавляем 1 к обработанному смещению. Т.о. храним следующий к обработке offset
                        new OffsetAndMetadata(o.getOffset() + 1)
                )
        );
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (consumer) {
            consumer.commitSync(map);
        }
    }
}
