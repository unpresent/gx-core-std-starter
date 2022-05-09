import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.annotation.Testable;
import ru.gx.core.channels.ChannelDirection;
import ru.gx.core.kafka.offsets.TopicPartitionOffset;
import ru.gx.core.std.offsets.FileTopicsOffsetsStorage;

import java.util.ArrayList;

@Testable
public class TestFileTopicsOffsets extends FileTopicsOffsetsStorage {

    public TestFileTopicsOffsets(
            @NotNull final ObjectMapper objectMapper
    ) {
        super("offsets.data", objectMapper);
    }

    @Test
    public void doTestFileOffsets() {
        this.init();

        final var offsets1 = this.loadOffsets(ChannelDirection.In, "testService");
        offsets1.forEach(System.out::println);

        final var offsets2 = new ArrayList<TopicPartitionOffset>();
        offsets2.add(new TopicPartitionOffset("test-topic", 0, 1));
        offsets2.add(new TopicPartitionOffset("test-topic", 1, 101));

        this.saveOffsets(ChannelDirection.Out, "testService", offsets2);

        final var offsets3 = this.loadOffsets(ChannelDirection.Out, "testService");
        offsets3.forEach(System.out::println);
    }
}
