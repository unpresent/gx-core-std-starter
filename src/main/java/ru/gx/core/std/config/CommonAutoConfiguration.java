package ru.gx.core.std.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.gx.core.kafka.offsets.TopicsOffsetsController;
import ru.gx.core.std.offsets.FileTopicsOffsetsController;
import ru.gx.core.std.offsets.JdbcTopicsOffsetsController;
import ru.gx.core.std.offsets.JpaTopicsOffsetsController;

@Configuration
@EnableConfigurationProperties(ConfigurationPropertiesService.class)
public class CommonAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.offsets-controller.type", havingValue = "file")
    public TopicsOffsetsController fileTopicsOffsetsController() {
        return new FileTopicsOffsetsController();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.offsets-controller.type", havingValue = "jdbc")
    public TopicsOffsetsController jdbcTopicsOffsetsLoader() {
        return new JdbcTopicsOffsetsController();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.offsets-controller.type", havingValue = "jpa")
    public TopicsOffsetsController jpaTopicsOffsetsController() {
        return new JpaTopicsOffsetsController();
    }
}
