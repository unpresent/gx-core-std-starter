package ru.gx.std.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.gx.std.entities.KafkaSnapshotPublishedOffsetEntity;

@SuppressWarnings("unused")
@Repository
public interface KafkaSnapshotPublishedOffsetsRepository extends JpaRepository<KafkaSnapshotPublishedOffsetEntity, String> {
}
