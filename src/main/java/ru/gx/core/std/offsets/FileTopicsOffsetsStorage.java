package ru.gx.core.std.offsets;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import net.minidev.json.annotate.JsonIgnore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Value;
import ru.gx.core.channels.ChannelDirection;
import ru.gx.core.data.AbstractDataObject;
import ru.gx.core.data.AbstractDataPackage;
import ru.gx.core.kafka.KafkaConstants;
import ru.gx.core.kafka.offsets.TopicPartitionOffset;
import ru.gx.core.kafka.offsets.TopicsOffsetsStorage;
import ru.gx.core.messaging.Message;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.*;

import static lombok.AccessLevel.PROTECTED;

@Slf4j
public class FileTopicsOffsetsStorage implements TopicsOffsetsStorage {
    @Getter(value = PROTECTED)
    @NotNull
    private final ObjectMapper objectMapper;

    @Getter(value = PROTECTED)
    @NotNull
    private final String fileStorageName;

    private File fileStorage;

    private final Map<String, ReaderOffsets> readerOffsets = new HashMap<>();

    public FileTopicsOffsetsStorage(
            @NotNull @Value("${service.kafka.offsets-storage.file-storage}") final String fileStorageName,
            @NotNull final ObjectMapper objectMapper
    ) {
        this.objectMapper = objectMapper;
        this.fileStorageName = fileStorageName;
    }

    @SneakyThrows({FileNotFoundException.class, IOException.class})
    @SuppressWarnings("ResultOfMethodCallIgnored")
    @PostConstruct
    protected void init() {
        this.fileStorage = new File(fileStorageName);
        if (!this.fileStorage.canRead()) {
            this.fileStorage.setReadable(true);
        }
        if (!this.fileStorage.canWrite()) {
            this.fileStorage.setWritable(true);
        }

        this.readerOffsets.clear();
        if (this.fileStorage.exists()) {
            try (final var stream = new FileInputStream(this.fileStorage)) {
                final var readers = this.objectMapper.readValue(stream, ReaderOffsetsPackage.class);
                for (var reader : readers.getObjects()) {
                    this.readerOffsets.put(reader.getReaderName(), reader);
                }
            }
        }
    }

    @Override
    @Nullable
    public Collection<TopicPartitionOffset> loadOffsets(
            @NotNull final ChannelDirection direction,
            @NotNull final String readerName
    ) {
        var reader = this.readerOffsets.get(readerName);
        if (reader == null) {
            return new ArrayList<>();
        }

        if (direction == ChannelDirection.In) {
            return reader.getIncomeOffsets();
        } else {
            return reader.getOutcomeOffsets();
        }
    }

    @Override
    public void saveOffsets(@NotNull final ChannelDirection direction, @NotNull final String readerName, @NotNull final Collection<TopicPartitionOffset> offsets) {
        try {
            var reader = this.readerOffsets.get(readerName);
            if (reader == null) {
                reader = new ReaderOffsets().setReaderName(readerName);
                this.readerOffsets.put(readerName, reader);
            }

            if (direction == ChannelDirection.In) {
                reader.getIncomeOffsets().clear();
                reader.getIncomeOffsets().addAll(offsets);
            } else {
                reader.getOutcomeOffsets().clear();
                reader.getOutcomeOffsets().addAll(offsets);
            }

            try (final var out = new FileOutputStream(this.fileStorage)) {
                final var readers = new ReaderOffsetsPackage();
                readers.getObjects().addAll(this.readerOffsets.values());
                this.objectMapper.writeValue(out, readers);
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }


    @Override
    public void saveOffsetFromMessage(
            @NotNull final ChannelDirection channelDirection,
            @NotNull final String serviceName,
            @NotNull final Message<?> message
    ) {
        final var partition = (Integer) message.getMetadataValue(KafkaConstants.METADATA_PARTITION);
        if (partition == null) {
            throw new NullPointerException("Message doesn't have metadata " + KafkaConstants.METADATA_PARTITION + "!");
        }
        final var offset = (Long) message.getMetadataValue(KafkaConstants.METADATA_OFFSET);
        if (offset == null) {
            throw new NullPointerException("Message doesn't have metadata " + KafkaConstants.METADATA_OFFSET + "!");
        }
        final var topicName = message.getChannelDescriptor().getApi().getName();
        saveOffsets(
                ChannelDirection.In,
                serviceName,
                Collections.singletonList(new TopicPartitionOffset(topicName, partition, offset))
        );
    }

    @Accessors(chain = true)
    public static class ReaderOffsets extends AbstractDataObject {
        @Getter
        @Setter
        @NotNull
        private String readerName;

        @JsonIgnore
        @NotNull
        private final List<TopicPartitionOffset> incomeOffsets = new ArrayList<>();

        @JsonIgnore
        @NotNull
        private final List<TopicPartitionOffset> outcomeOffsets = new ArrayList<>();

        @JsonProperty("incomeOffsets")
        public Collection<TopicPartitionOffset> getIncomeOffsets() {
            return this.incomeOffsets;
        }

        @JsonProperty("outcomeOffsets")
        public Collection<TopicPartitionOffset> getOutcomeOffsets() {
            return this.outcomeOffsets;
        }
    }

    private static class ReaderOffsetsPackage extends AbstractDataPackage<ReaderOffsets> {
    }
}
