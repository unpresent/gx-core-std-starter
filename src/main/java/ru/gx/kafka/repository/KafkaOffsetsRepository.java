package ru.gx.kafka.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.gx.kafka.entities.KafkaIncomeOffsetEntity;
import ru.gx.kafka.entities.KafkaIncomeOffsetEntityId;

@SuppressWarnings("unused")
@Repository
// @ConditionalOnProperty(value = "simple-kafka-income-offset-controller.enabled", havingValue = "true")
public interface KafkaOffsetsRepository extends JpaRepository<KafkaIncomeOffsetEntity, KafkaIncomeOffsetEntityId> {
}
