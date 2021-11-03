package ru.gx.std.offsets;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import ru.gx.data.ActiveConnectionsContainer;
import ru.gx.kafka.TopicDirection;
import ru.gx.kafka.offsets.TopicPartitionOffset;
import ru.gx.kafka.offsets.TopicsOffsetsLoader;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import static lombok.AccessLevel.PROTECTED;

public class JdbcTopicsOffsetsLoader implements TopicsOffsetsLoader {

    @Getter(PROTECTED)
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    private ActiveConnectionsContainer connectionsContainer;

    @SneakyThrows(SQLException.class)
    @Override
    public Collection<TopicPartitionOffset> loadOffsets(@NotNull final TopicDirection direction, @NotNull final String serviceName) {
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

    private Connection getCheckedConnection() throws SQLException {
        final var result = this.connectionsContainer.getCurrent();
        if (result == null) {
            throw new SQLException("Connection in current thread doesn't opened!");
        }
        return result;
    }
}
