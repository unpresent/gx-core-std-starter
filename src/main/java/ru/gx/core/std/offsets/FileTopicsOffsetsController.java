package ru.gx.core.std.offsets;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import net.minidev.json.annotate.JsonIgnore;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import ru.gx.core.channels.ChannelDirection;
import ru.gx.core.data.AbstractDataObject;
import ru.gx.core.data.AbstractDataPackage;
import ru.gx.core.kafka.offsets.TopicPartitionOffset;
import ru.gx.core.kafka.offsets.TopicsOffsetsController;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.*;

import static lombok.AccessLevel.PROTECTED;

public class FileTopicsOffsetsController implements TopicsOffsetsController {
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    private ObjectMapper objectMapper;

    @Getter(value = PROTECTED)
    @Setter(value = PROTECTED)
    @Value("${service.offsets-controller.file-storage}")
    private String fileStorageName;

    private File fileStorage;

    private final Map<String, ReaderOffsets> readerOffsets = new HashMap<>();

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

    @SneakyThrows({FileNotFoundException.class, IOException.class})
    @Override
    public void saveOffsets(@NotNull final ChannelDirection direction, @NotNull final String readerName, @NotNull final Collection<TopicPartitionOffset> offsets) {
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
    }

    @Override
    public Collection<TopicPartitionOffset> loadOffsets(@NotNull final ChannelDirection direction, @NotNull final String readerName) {
        var reader = this.readerOffsets.get(readerName);
        if (reader == null) {
            return null;
        }

        if (direction == ChannelDirection.In) {
            return reader.getIncomeOffsets();
        } else {
            return reader.getOutcomeOffsets();
        }
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
