package ru.gx.kafka.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.gx.kafka.entities.KafkaSnapshotPublishedOffsetEntity;

@SuppressWarnings("unused")
@Repository
// @ConditionalOnProperty(value = "simple-kafka-income-published-controller.enabled", havingValue = "true")
public interface KafkaSnapshotPublishedOffsetsRepository extends JpaRepository<KafkaSnapshotPublishedOffsetEntity, String> {
}
