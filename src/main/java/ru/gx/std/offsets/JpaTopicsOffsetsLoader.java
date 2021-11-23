package ru.gx.std.offsets;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.hibernate.Session;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import ru.gx.channels.ChannelDirection;
import ru.gx.data.ActiveSessionsContainer;
import ru.gx.kafka.offsets.TopicPartitionOffset;
import ru.gx.kafka.offsets.TopicsOffsetsLoader;

import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static lombok.AccessLevel.PROTECTED;

public class JpaTopicsOffsetsLoader implements TopicsOffsetsLoader {
    @Getter(PROTECTED)
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    private ActiveSessionsContainer sessionsContainer;

    @SuppressWarnings("unchecked")
    @SneakyThrows(SQLException.class)
    @Override
    public Collection<TopicPartitionOffset> loadOffsets(@NotNull final ChannelDirection direction, @NotNull final String serviceName) {
        final var session = getCheckedSession();

        final var result = new ArrayList<TopicPartitionOffset>();
        final var stmt = session.createSQLQuery(TopicsOffsetsSql.Load.SQL);
        stmt.setParameter(TopicsOffsetsSql.Load.PARAM_INDEX_DIRECTION, direction.name());
        stmt.setParameter(TopicsOffsetsSql.Load.PARAM_INDEX_SERVICE_NAME, serviceName);
        List<Object[]> rows = stmt.list();
        for (var row : rows) {
            final var offset = (BigInteger)row[TopicsOffsetsSql.Load.COLUMN_INDEX_OFFSET-1];
            result.add(new TopicPartitionOffset(
                    (String)row[TopicsOffsetsSql.Load.COLUMN_INDEX_TOPIC-1],
                    (Integer)row[TopicsOffsetsSql.Load.COLUMN_INDEX_PARTITION-1],
                    offset.longValueExact()
            ));
        }
        return result;
    }

    private Session getCheckedSession() throws SQLException {
        final var result = this.sessionsContainer.getCurrent();
        if (result == null) {
            throw new SQLException("Session in current thread doesn't opened!");
        }
        return result;
    }
}
