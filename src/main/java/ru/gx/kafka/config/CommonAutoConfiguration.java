package ru.gx.kafka.config;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import ru.gx.kafka.load.SimpleKafkaIncomeOffsetsController;
import ru.gx.kafka.upload.SimpleOutcomeTopicUploader;

@Configuration
@EnableJpaRepositories({"ru.gx.kafka.repository"})
@EntityScan({"ru.gx.kafka.entities"})
public class CommonAutoConfiguration {
    @Bean
    // @ConditionalOnProperty(value = "simple-kafka-income-offsets-controller.enabled", havingValue = "true")
    public SimpleKafkaIncomeOffsetsController simpleKafkaIncomeOffsetsController() {
        return new SimpleKafkaIncomeOffsetsController();
    }

    @Bean
    public SimpleOutcomeTopicUploader simpleOutcomeTopicUploader() {
        return new SimpleOutcomeTopicUploader();
    }
}
