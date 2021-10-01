package ru.gx.std.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.gx.std.entities.KafkaIncomeOffsetEntity;
import ru.gx.std.entities.KafkaIncomeOffsetEntityId;

@SuppressWarnings("unused")
@Repository
public interface KafkaOffsetsRepository extends JpaRepository<KafkaIncomeOffsetEntity, KafkaIncomeOffsetEntityId> {
}
