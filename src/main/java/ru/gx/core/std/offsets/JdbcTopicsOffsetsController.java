package ru.gx.core.std.offsets;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import ru.gx.core.channels.ChannelDirection;
import ru.gx.core.data.ActiveConnectionsContainer;
import ru.gx.core.kafka.offsets.TopicPartitionOffset;
import ru.gx.core.kafka.offsets.TopicsOffsetsController;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import static lombok.AccessLevel.PROTECTED;

public class JdbcTopicsOffsetsController implements TopicsOffsetsController {

    @Getter(PROTECTED)
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    private ActiveConnectionsContainer connectionsContainer;

    @SneakyThrows(SQLException.class)
    @Override
    @NotNull
    public Collection<TopicPartitionOffset> loadOffsets(@NotNull final ChannelDirection direction, @NotNull final String serviceName) {
        final var connection = getCheckedConnection();

        final var result = new ArrayList<TopicPartitionOffset>();
        try (final var stmt = connection.prepareStatement(TopicsOffsetsSql.Load.SQL)) {
            stmt.setString(TopicsOffsetsSql.Load.PARAM_INDEX_DIRECTION, direction.name());
            stmt.setString(TopicsOffsetsSql.Load.PARAM_INDEX_SERVICE_NAME, serviceName);
            final var rs = stmt.executeQuery();
            while (rs.next()) {
                result.add(new TopicPartitionOffset(
                        rs.getString(TopicsOffsetsSql.Load.COLUMN_INDEX_TOPIC),
                        rs.getInt(TopicsOffsetsSql.Load.COLUMN_INDEX_PARTITION),
                        rs.getLong(TopicsOffsetsSql.Load.COLUMN_INDEX_OFFSET)
                ));
            }
        }
        return result;
    }

    @SneakyThrows(SQLException.class)
    @Override
    public void saveOffsets(@NotNull final ChannelDirection direction, @NotNull final String readerName, @NotNull final Collection<TopicPartitionOffset> offsets) {
        final var connection = getCheckedConnection();
        try (final var stmt = connection.prepareStatement(TopicsOffsetsSql.Save.SQL)) {
            for (var item : offsets) {
                stmt.setString(TopicsOffsetsSql.Save.PARAM_INDEX_DIRECTION, direction.name());
                stmt.setString(TopicsOffsetsSql.Save.PARAM_INDEX_READER, readerName);
                stmt.setString(TopicsOffsetsSql.Save.PARAM_INDEX_TOPIC, item.getTopic());
                stmt.setInt(TopicsOffsetsSql.Save.PARAM_INDEX_PARTITION, item.getPartition());
                stmt.setLong(TopicsOffsetsSql.Save.PARAM_INDEX_OFFSET, item.getOffset());
                stmt.execute();
            }
        }
    }

    private Connection getCheckedConnection() throws SQLException {
        final var result = this.connectionsContainer.getCurrent();
        if (result == null) {
            throw new SQLException("Connection in current thread doesn't opened!");
        }
        return result;
    }
}
