package ru.gx.std.offsets;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.hibernate.Session;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import ru.gx.data.ActiveConnectionsContainer;
import ru.gx.data.ActiveSessionsContainer;
import ru.gx.kafka.TopicDirection;
import ru.gx.kafka.offsets.TopicPartitionOffset;
import ru.gx.kafka.offsets.TopicsOffsetsSaver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

import static lombok.AccessLevel.PROTECTED;

public class JpaTopicsOffsetsSaver implements TopicsOffsetsSaver {
    @Getter(PROTECTED)
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    private ActiveSessionsContainer sessionsContainer;

    @SneakyThrows(SQLException.class)
    @Override
    public void saveOffsets(@NotNull final TopicDirection direction, @NotNull final String readerName, @NotNull final Collection<TopicPartitionOffset> offsets) {
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
