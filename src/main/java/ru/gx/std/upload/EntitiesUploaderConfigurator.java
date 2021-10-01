package ru.gx.std.upload;

import org.jetbrains.annotations.NotNull;

/**
 * Реализатор данного интерфейса будет вызван после настройки всех бинов (во время обработки ApplicationReadyEvent).
 * Задача реализатора данного интерфейса заключается в конфигурировании исходящих потоков из БД в Kafka.
 */
@SuppressWarnings("unused")
public interface EntitiesUploaderConfigurator {
    /**
     * Вызывается после настройки бинов (в BeanPostProcessor-е).
     * @param uploader Передается бин, реализующий интерфейс EntitiesUploader. Данный бин в методе реализации требуется настроить.
     */
    void configureEntitiesUploader(@NotNull final EntitiesUploader uploader);
}
