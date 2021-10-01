package ru.gx.std.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import ru.gx.std.load.SimpleKafkaIncomeOffsetsController;
import ru.gx.std.upload.EntitiesUploaderConfiguratorCaller;
import ru.gx.std.upload.SimpleEntitiesUploader;

@Configuration
@EnableJpaRepositories({"ru.gx.std.repository"})
@EntityScan({"ru.gx.std.entities"})
public class CommonAutoConfiguration {
    public CommonAutoConfiguration() {
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.income-topics.simple-offsets-controller.enabled", havingValue = "true")
    public SimpleKafkaIncomeOffsetsController simpleKafkaIncomeOffsetsController() {
        return new SimpleKafkaIncomeOffsetsController();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.outcome-entities.simple-uploader.enabled", havingValue = "true")
    public SimpleEntitiesUploader simpleEntitiesUploader() {
        return new SimpleEntitiesUploader();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.outcome-entities.configurator-caller.enabled", havingValue = "true")
    public EntitiesUploaderConfiguratorCaller entitiesUploaderConfiguratorCaller() {
        return new EntitiesUploaderConfiguratorCaller();
    }
}
