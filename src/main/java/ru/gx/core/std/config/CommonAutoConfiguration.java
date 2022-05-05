package ru.gx.core.std.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.gx.core.data.sqlwrapping.ThreadConnectionsWrapper;
import ru.gx.core.kafka.offsets.TopicsOffsetsStorage;
import ru.gx.core.std.offsets.SqlTopicsOffsetsStorage;
import ru.gx.core.std.offsets.FileTopicsOffsetsStorage;

import javax.sql.DataSource;

@Configuration
@EnableConfigurationProperties({ConfigurationPropertiesService.class})
public class CommonAutoConfiguration {

    @Bean
    @ConditionalOnBean(name = "offsetsDataSource")
    @ConditionalOnProperty(prefix = "spring.datasource-for-offsets", name = "url")
    public NamedParameterJdbcOperations namedParameterJdbcOperationsForOffsets(@Qualifier("offsetsDataSource") DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.offsets-storage.type", havingValue = "file")
    public TopicsOffsetsStorage fileTopicsOffsetsController(@NotNull final ObjectMapper objectMapper) {
        return new FileTopicsOffsetsStorage(objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.offsets-storage.type", havingValue = "sql")
    public TopicsOffsetsStorage dbTopicsOffsetsLoader(@NotNull final ThreadConnectionsWrapper connectionsWrapper) {
        return new SqlTopicsOffsetsStorage(connectionsWrapper);
    }
}