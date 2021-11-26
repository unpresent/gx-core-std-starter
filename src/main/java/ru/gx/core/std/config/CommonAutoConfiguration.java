package ru.gx.core.std.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.gx.core.kafka.offsets.TopicsOffsetsLoader;
import ru.gx.core.kafka.offsets.TopicsOffsetsSaver;
import ru.gx.core.std.offsets.JdbcTopicsOffsetsLoader;
import ru.gx.core.std.offsets.JdbcTopicsOffsetsSaver;
import ru.gx.core.std.offsets.JpaTopicsOffsetsLoader;
import ru.gx.core.std.offsets.JpaTopicsOffsetsSaver;

@Configuration
@EnableConfigurationProperties(ConfigurationPropertiesService.class)
public class CommonAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.offsets-loaders.native-jdbc.enabled", havingValue = "true")
    public TopicsOffsetsLoader jdbcTopicsOffsetsLoader() {
        return new JdbcTopicsOffsetsLoader();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.offsets-loaders.jpa.enabled", havingValue = "true")
    public TopicsOffsetsLoader jpaTopicsOffsetsLoader() {
        return new JpaTopicsOffsetsLoader();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.offsets-savers.native-jdbc.enabled", havingValue = "true")
    public TopicsOffsetsSaver jdbcTopicsOffsetsSaver() {
        return new JdbcTopicsOffsetsSaver();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.offsets-savers.jpa.enabled", havingValue = "true")
    public TopicsOffsetsSaver jpaTopicsOffsetsSaver() {
        return new JpaTopicsOffsetsSaver();
    }
}
