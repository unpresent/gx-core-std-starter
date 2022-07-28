package ru.gx.core.std.offsets;

import org.jetbrains.annotations.NotNull;
import ru.gx.core.channels.ChannelDirection;
import ru.gx.core.kafka.KafkaConstants;
import ru.gx.core.kafka.load.KafkaIncomeTopicLoadingDescriptor;
import ru.gx.core.kafka.offsets.TopicPartitionOffset;
import ru.gx.core.kafka.offsets.TopicsOffsetsStorage;
import ru.gx.core.messaging.Message;

import java.util.Collections;

public abstract class AbstractTopicsOffsetsStorage implements TopicsOffsetsStorage {

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
        if (!(message.getChannelDescriptor() instanceof final KafkaIncomeTopicLoadingDescriptor kafkaDescriptor)) {
            throw new UnsupportedOperationException(
                    "Unsupported configuration descriptor " + message.getChannelDescriptor().getClass().getName()
                            + " for " + this.getClass().getName()
            );
        }
        final var topicOffset = new TopicPartitionOffset(topicName, partition, offset);
        saveOffsets(
                ChannelDirection.In,
                serviceName,
                message.getChannelDescriptor().getOwner(),
                Collections.singletonList(topicOffset)
        );
        kafkaDescriptor.setOffset(topicOffset.getPartition(), topicOffset.getOffset());
    }

}
