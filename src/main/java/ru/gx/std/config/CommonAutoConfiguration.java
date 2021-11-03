package ru.gx.std.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import ru.gx.kafka.offsets.TopicsOffsetsLoader;
import ru.gx.kafka.offsets.TopicsOffsetsSaver;
import ru.gx.std.offsets.JdbcTopicsOffsetsLoader;
import ru.gx.std.offsets.JdbcTopicsOffsetsSaver;
import ru.gx.std.offsets.JpaTopicsOffsetsLoader;
import ru.gx.std.offsets.JpaTopicsOffsetsSaver;

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
