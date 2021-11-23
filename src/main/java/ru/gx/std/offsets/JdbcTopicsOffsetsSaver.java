package ru.gx.std.offsets;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import ru.gx.channels.ChannelDirection;
import ru.gx.data.ActiveConnectionsContainer;
import ru.gx.kafka.offsets.TopicPartitionOffset;
import ru.gx.kafka.offsets.TopicsOffsetsSaver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

import static lombok.AccessLevel.PROTECTED;

public class JdbcTopicsOffsetsSaver implements TopicsOffsetsSaver {
    @Getter(PROTECTED)
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    private ActiveConnectionsContainer connectionsContainer;

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
