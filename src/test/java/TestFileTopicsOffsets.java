import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.annotation.Testable;
import ru.gx.core.channels.ChannelDirection;
import ru.gx.core.kafka.offsets.TopicPartitionOffset;
import ru.gx.core.std.offsets.FileTopicsOffsetsController;

import java.util.ArrayList;

@Testable
public class TestFileTopicsOffsets extends FileTopicsOffsetsController {

    public TestFileTopicsOffsets() {
        this.setObjectMapper(new ObjectMapper().registerModules(new JavaTimeModule()));
        setFileStorageName("offsets.data");
    }

    @Test
    public void doTestFileOffsets() {
        this.init();

        final var offsets1 = this.loadOffsets(ChannelDirection.In, "testService");
        if (offsets1 != null) {
            offsets1.forEach(System.out::println);
        }

        final var offsets2 = new ArrayList<TopicPartitionOffset>();
        offsets2.add(new TopicPartitionOffset("test-topic", 0, 1));
        offsets2.add(new TopicPartitionOffset("test-topic", 1, 101));

        this.saveOffsets(ChannelDirection.Out, "testService", offsets2);

        final var offsets3 = this.loadOffsets(ChannelDirection.Out, "testService");
        if (offsets3 != null) {
            offsets3.forEach(System.out::println);
        }
    }
}
