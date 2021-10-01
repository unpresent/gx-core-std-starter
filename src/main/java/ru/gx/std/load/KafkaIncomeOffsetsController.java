package ru.gx.std.load;

@SuppressWarnings("unused")
public interface KafkaIncomeOffsetsController {

    /**
     * Позиционирование в Kafka очередях при запуске
     */
    void seekIncomeOffsetsOnStart();

    /**
     * Сохранение в БД текущих смещений Kafka.
     */
    void saveKafkaOffsets();
}
