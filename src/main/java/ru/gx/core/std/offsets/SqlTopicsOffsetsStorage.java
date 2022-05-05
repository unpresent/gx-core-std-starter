package ru.gx.core.std.offsets;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import ru.gx.core.channels.ChannelDirection;
import ru.gx.core.data.sqlwrapping.ThreadConnectionsWrapper;
import ru.gx.core.kafka.offsets.TopicPartitionOffset;
import ru.gx.core.kafka.offsets.TopicsOffsetsStorage;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

import static lombok.AccessLevel.PROTECTED;

@RequiredArgsConstructor
public class SqlTopicsOffsetsStorage implements TopicsOffsetsStorage {

    @Getter(PROTECTED)
    @NotNull
    private final ThreadConnectionsWrapper threadConnectionsWrapper;

    @SneakyThrows({SQLException.class, IOException.class})
    @Override
    @NotNull
    public Collection<TopicPartitionOffset> loadOffsets(@NotNull final ChannelDirection direction, @NotNull final String serviceName) {
        final var connectionWrapper = this.threadConnectionsWrapper.getCurrentThreadConnection();

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
    }

    @SneakyThrows({SQLException.class, IOException.class})
    @Override
    public void saveOffsets(@NotNull final ChannelDirection direction, @NotNull final String readerName, @NotNull final Collection<TopicPartitionOffset> offsets) {
        final var connectionWrapper = this.threadConnectionsWrapper.getCurrentThreadConnection();
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
    }
}
