package ru.gx.core.std.offsets;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.gx.core.channels.ChannelDirection;
import ru.gx.core.data.sqlwrapping.ThreadConnectionsWrapper;
import ru.gx.core.kafka.KafkaConstants;
import ru.gx.core.kafka.offsets.TopicPartitionOffset;
import ru.gx.core.kafka.offsets.TopicsOffsetsStorage;
import ru.gx.core.messaging.Message;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

import static lombok.AccessLevel.PROTECTED;

@Slf4j
@RequiredArgsConstructor
public class SqlTopicsOffsetsStorage implements TopicsOffsetsStorage {

    @Getter(PROTECTED)
    @NotNull
    private final ThreadConnectionsWrapper threadConnectionsWrapper;

    @Override
    @Nullable
    public Collection<TopicPartitionOffset> loadOffsets(@NotNull final ChannelDirection direction, @NotNull final String serviceName) {
        try (final var connectionWrapper = this.threadConnectionsWrapper.getCurrentThreadConnection()) {

            final var result = new ArrayList<TopicPartitionOffset>();
            try (final var commandWrapper = connectionWrapper.getQuery(TopicsOffsetsSql.Load.SQL)) {
                commandWrapper.setStringParam(TopicsOffsetsSql.Load.PARAM_INDEX_DIRECTION, direction.name());
                commandWrapper.setStringParam(TopicsOffsetsSql.Load.PARAM_INDEX_SERVICE_NAME, serviceName);
                final var rs = commandWrapper.executeWithResult();
                while (rs.next()) {
                    result.add(new TopicPartitionOffset(
                            Objects.requireNonNull(rs.getString(TopicsOffsetsSql.Load.COLUMN_INDEX_TOPIC)),
                            Objects.requireNonNull(rs.getInteger(TopicsOffsetsSql.Load.COLUMN_INDEX_PARTITION)),
                            Objects.requireNonNull(rs.getLong(TopicsOffsetsSql.Load.COLUMN_INDEX_OFFSET))
                    ));
                }
            }
            return result;

        } catch (SQLException | IOException e) {
            log.error("", e);
            return null;
        }
    }

    @Override
    public void saveOffsets(
            @NotNull final ChannelDirection direction,
            @NotNull final String readerName,
            @NotNull final Collection<TopicPartitionOffset> offsets
    ) {
        try (final var connectionWrapper = this.threadConnectionsWrapper.getCurrentThreadConnection()) {
            try (final var commandWrapper = connectionWrapper.getQuery(TopicsOffsetsSql.Save.SQL)) {
                for (var item : offsets) {
                    commandWrapper.setStringParam(TopicsOffsetsSql.Save.PARAM_INDEX_DIRECTION, direction.name());
                    commandWrapper.setStringParam(TopicsOffsetsSql.Save.PARAM_INDEX_READER, readerName);
                    commandWrapper.setStringParam(TopicsOffsetsSql.Save.PARAM_INDEX_TOPIC, item.getTopic());
                    commandWrapper.setIntegerParam(TopicsOffsetsSql.Save.PARAM_INDEX_PARTITION, item.getPartition());
                    commandWrapper.setLongParam(TopicsOffsetsSql.Save.PARAM_INDEX_OFFSET, item.getOffset());
                    commandWrapper.executeNoResult();
                }
            }
        } catch (SQLException | IOException e) {
            log.error("", e);
        }
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
        final var topicName = message.getChannelDescriptor().getApi().getName();
        saveOffsets(
                ChannelDirection.In,
                serviceName,
                Collections.singletonList(new TopicPartitionOffset(topicName, partition, offset))
        );
    }
}
