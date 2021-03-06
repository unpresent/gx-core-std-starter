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
import ru.gx.core.kafka.load.KafkaIncomeTopicLoadingDescriptor;
import ru.gx.core.kafka.offsets.PartitionOffset;
import ru.gx.core.kafka.offsets.TopicPartitionOffset;
import ru.gx.core.kafka.offsets.TopicsOffsetsStorage;

import java.util.*;

@Slf4j
@RequiredArgsConstructor
public class KafkaTopicsOffsetsStorage extends AbstractTopicsOffsetsStorage implements TopicsOffsetsStorage {
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
            // ?????????????????????? ???? ???????????????????????? ????????????????????????
            final var kafkaDescriptor = (KafkaIncomeTopicLoadingDescriptor) descriptor;
            final var localOffsets = new ArrayList<PartitionOffset>();
            kafkaDescriptor.getTopicPartitions().forEach(topicPartition -> {
                // ?????????????????????? ???? ???????? ?????????? <Partition, Offset>
                offsets.forEach(offset -> {
                    if (offset.getTopic().equals(topicPartition.topic())
                            && offset.getPartition() == topicPartition.partition()) {
                        // ???????????????? ???? ?????????? ???????? offsets (????. ????????????????) ???????????? ????????????????????
                        localOffsets.add(new PartitionOffset(offset.getPartition(), offset.getOffset()));
                    }
                });
            });
            // ?? ?????????????????? ????????????????????
            internalSaveOffsets(kafkaDescriptor.getChannelName(), kafkaDescriptor.getConsumer(), localOffsets);
        });
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
                        // ?????????????????? 1 ?? ?????????????????????????? ????????????????. ??.??. ???????????? ?????????????????? ?? ?????????????????? offset
                        new OffsetAndMetadata(o.getOffset() + 1)
                )
        );
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (consumer) {
            consumer.commitSync(map);
        }
    }
}
