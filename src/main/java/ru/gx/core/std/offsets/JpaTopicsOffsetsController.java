package ru.gx.core.std.offsets;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.hibernate.Session;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import ru.gx.core.channels.ChannelDirection;
import ru.gx.core.data.ActiveSessionsContainer;
import ru.gx.core.kafka.offsets.TopicPartitionOffset;
import ru.gx.core.kafka.offsets.TopicsOffsetsController;

import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static lombok.AccessLevel.PROTECTED;

public class JpaTopicsOffsetsController implements TopicsOffsetsController {
    @Getter(PROTECTED)
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    private ActiveSessionsContainer sessionsContainer;

    @SuppressWarnings("unchecked")
    @SneakyThrows(SQLException.class)
    @Override
    @NotNull
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

    @SneakyThrows(SQLException.class)
    @Override
    public void saveOffsets(@NotNull final ChannelDirection direction, @NotNull final String readerName, @NotNull final Collection<TopicPartitionOffset> offsets) {
        final var session = getCheckedSession();
        final var stmt = session.createSQLQuery(TopicsOffsetsSql.Save.SQL);
        for (var item : offsets) {
            stmt.setParameter(TopicsOffsetsSql.Save.PARAM_INDEX_DIRECTION, direction.name());
            stmt.setParameter(TopicsOffsetsSql.Save.PARAM_INDEX_READER, readerName);
            stmt.setParameter(TopicsOffsetsSql.Save.PARAM_INDEX_TOPIC, item.getTopic());
            stmt.setParameter(TopicsOffsetsSql.Save.PARAM_INDEX_PARTITION, item.getPartition());
            stmt.setParameter(TopicsOffsetsSql.Save.PARAM_INDEX_OFFSET, item.getOffset());
            stmt.executeUpdate();
        }
    }

    private Session getCheckedSession() throws SQLException {
        final var result = this.sessionsContainer.getCurrent();
        if (result == null) {
            throw new SQLException("Session in current thread doesn't opened!");
        }
        return result;
    }
}
