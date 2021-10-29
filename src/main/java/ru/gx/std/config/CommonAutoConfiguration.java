package ru.gx.std.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import ru.gx.std.offsets.JdbcTopicsOffsetsLoader;
import ru.gx.std.offsets.JdbcTopicsOffsetsSaver;

@Configuration
@EntityScan({"ru.gx.std.entities"})
public class CommonAutoConfiguration {
    public CommonAutoConfiguration() {
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.income-topics.jdbc-saver.enabled", havingValue = "true")
    public JdbcTopicsOffsetsSaver jdbcIncomeTopicsOffsetsSaver() {
        return new JdbcTopicsOffsetsSaver();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.income-topics.jdbc-loader.enabled", havingValue = "true")
    public JdbcTopicsOffsetsLoader jdbcIncomeTopicsOffsetsLoader() {
        return new JdbcTopicsOffsetsLoader();
    }
}
