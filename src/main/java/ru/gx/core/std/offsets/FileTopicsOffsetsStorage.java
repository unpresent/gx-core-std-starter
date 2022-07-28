package ru.gx.core.std.offsets;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import net.minidev.json.annotate.JsonIgnore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Value;
import ru.gx.core.channels.ChannelDirection;
import ru.gx.core.channels.ChannelsConfiguration;
import ru.gx.core.data.AbstractDataObject;
import ru.gx.core.data.AbstractDataPackage;
import ru.gx.core.kafka.offsets.TopicPartitionOffset;
import ru.gx.core.kafka.offsets.TopicsOffsetsStorage;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.*;

import static lombok.AccessLevel.PROTECTED;

@Slf4j
public class FileTopicsOffsetsStorage extends AbstractTopicsOffsetsStorage implements TopicsOffsetsStorage {
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
            @NotNull final String serviceName,
            @NotNull final ChannelsConfiguration configuration
    ) {
        var reader = this.readerOffsets.get(serviceName);
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
    public void saveOffsets(
            @NotNull final ChannelDirection direction,
            @NotNull final String serviceName,
            @NotNull final ChannelsConfiguration configuration,
            @NotNull final Collection<TopicPartitionOffset> offsets
    ) {
        try {
            var reader = this.readerOffsets.get(serviceName);
            if (reader == null) {
                reader = new ReaderOffsets().setReaderName(serviceName);
                this.readerOffsets.put(serviceName, reader);
            }

            if (direction == ChannelDirection.In) {
                reader.getIncomeOffsets().clear();
                reader.getIncomeOffsets().addAll(offsets);
                // Добавляем 1 к обработанному смещению. Т.о. храним следующий к обработке offset
                reader.getIncomeOffsets().forEach(offset -> offset.setOffset(offset.getOffset() + 1));
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
