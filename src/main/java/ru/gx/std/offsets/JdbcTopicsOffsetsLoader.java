package ru.gx.std.offsets;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import ru.gx.data.ActiveConnectionsContainer;
import ru.gx.kafka.TopicDirection;
import ru.gx.kafka.load.IncomeTopicsConfiguration;
import ru.gx.kafka.offsets.TopicPartitionOffset;
import ru.gx.kafka.offsets.TopicsOffsetsLoader;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import static lombok.AccessLevel.PROTECTED;

public class JdbcTopicsOffsetsLoader implements TopicsOffsetsLoader {
    private final static String sqlSelect =
            "SELECT\n" +
                    "    \"Topic\",\n" +
                    "    \"Partition\",\n" +
                    "    \"Offset\"\n" +
                    "FROM \"Kafka\".\"Offsets\"" +
                    "WHERE  \"Direction\" = ?" +
                    "   AND \"ServiceName\" = ?";
    private final static int columnIndexTopic = 1;
    private final static int columnIndexPartition = 2;
    private final static int columnIndexOffset = 3;
    private final static int paramIndexSelectDirection = 1;
    private final static int paramIndexSelectServiceName = 2;

    @Getter(PROTECTED)
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    private ActiveConnectionsContainer connectionsContainer;

    @SneakyThrows(SQLException.class)
    @Override
    public Collection<TopicPartitionOffset> loadOffsets(@NotNull final TopicDirection direction, @NotNull final String serviceName) {
        final var connection = getCheckedConnection();

        final var result = new ArrayList<TopicPartitionOffset>();
        try (final var stmt = connection.prepareStatement(sqlSelect)) {
            stmt.setString(paramIndexSelectDirection, direction.name());
            stmt.setString(paramIndexSelectServiceName, serviceName);
            final var rs = stmt.executeQuery();
            while (rs.next()) {
                result.add(new TopicPartitionOffset(
                        rs.getString(columnIndexTopic),
                        rs.getInt(columnIndexPartition),
                        rs.getLong(columnIndexOffset)
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
