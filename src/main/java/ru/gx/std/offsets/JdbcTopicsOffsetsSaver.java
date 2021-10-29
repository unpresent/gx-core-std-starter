package ru.gx.std.offsets;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import ru.gx.data.ActiveConnectionsContainer;
import ru.gx.kafka.TopicDirection;
import ru.gx.kafka.load.*;
import ru.gx.kafka.offsets.TopicPartitionOffset;
import ru.gx.kafka.offsets.TopicsOffsetsSaver;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import static lombok.AccessLevel.PROTECTED;

public class JdbcTopicsOffsetsSaver implements TopicsOffsetsSaver {
    private final static String sqlInsertAndUpdate =
            "INSERT INTO \"Kafka\".\"Offsets\" (\"Direction\", \"ServiceName\", \"Topic\", \"Partition\", \"Offset\")\n" +
                    "VALUES (?, ?, ?, ?, ?)\n" +
                    "ON CONFLICT (\"Direction\", \"ServiceName\", \"Topic\", \"Partition\") DO UPDATE SET\n" +
                    "    \"Offset\" = EXCLUDED.\"Offset\"";
    private final static int paramIndexInsertDirection = 1;
    private final static int paramIndexInsertReader = 2;
    private final static int paramIndexInsertTopic = 3;
    private final static int paramIndexInsertPartition = 4;
    private final static int paramIndexInsertOffset = 5;

    @Getter(PROTECTED)
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    private ActiveConnectionsContainer connectionsContainer;


    @SneakyThrows(SQLException.class)
    @Override
    public void saveOffsets(@NotNull final TopicDirection direction, @NotNull final String readerName, @NotNull final Collection<TopicPartitionOffset> offsets) {
        final var connection = getCheckedConnection();
        try (final var stmt = connection.prepareStatement(sqlInsertAndUpdate)) {
            for (var item : offsets) {
                stmt.setString(paramIndexInsertDirection, direction.name());
                stmt.setString(paramIndexInsertReader, readerName);
                stmt.setString(paramIndexInsertTopic, item.getTopic());
                stmt.setInt(paramIndexInsertPartition, item.getPartition());
                stmt.setLong(paramIndexInsertOffset, item.getOffset());
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
