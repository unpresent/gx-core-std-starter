package ru.gx.core.std.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.gx.core.data.sqlwrapping.ThreadConnectionsWrapper;
import ru.gx.core.kafka.offsets.TopicsOffsetsStorage;
import ru.gx.core.std.offsets.FileTopicsOffsetsStorage;
import ru.gx.core.std.offsets.SqlTopicsOffsetsStorage;

@Configuration
@EnableConfigurationProperties({ConfigurationPropertiesService.class})
public class CommonAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.kafka.offsets-storage.type", havingValue = "file")
    public TopicsOffsetsStorage fileTopicsOffsetsController(@NotNull final ObjectMapper objectMapper) {
        return new FileTopicsOffsetsStorage(objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.kafka.offsets-storage.type", havingValue = "sql")
    public TopicsOffsetsStorage dbTopicsOffsetsLoader(@NotNull final ThreadConnectionsWrapper connectionsWrapper) {
        return new SqlTopicsOffsetsStorage(connectionsWrapper);
    }
}