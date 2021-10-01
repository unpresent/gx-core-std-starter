package ru.gx.std.upload;

import com.fasterxml.jackson.databind.JsonMappingException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;
import ru.gx.data.ObjectAlreadyExistsException;
import ru.gx.data.ObjectNotExistsException;
import ru.gx.data.jpa.EntityObject;
import ru.gx.kafka.PartitionOffset;
import ru.gx.std.entities.KafkaSnapshotPublishedOffsetEntity;
import ru.gx.std.repository.KafkaSnapshotPublishedOffsetsRepository;
import ru.gx.kafka.upload.SimpleOutcomeTopicUploader;

import java.security.InvalidParameterException;
import java.util.*;

import static lombok.AccessLevel.PROTECTED;

@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
@Slf4j
@SuppressWarnings("unused")
public abstract class AbstractEntitiesUploader implements EntitiesUploader {
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    @Getter(PROTECTED)
    @NotNull
    private final Map<Class<? extends EntityObject>, Collection<EntityObject>> changesMap;

    @NotNull
    private final List<UploadingEntityDescriptor<? extends DataObject, ? extends DataPackage<DataObject>, ? extends EntityObject>> uploadingEntityDescriptors;

    @Getter
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    @NotNull
    private SimpleOutcomeTopicUploader simpleOutcomeTopicUploader;

    @NotNull
    private final Map<String, KafkaSnapshotPublishedOffsetEntity> kafkaSnapshotPublishedOffsetEntitiesCache = new HashMap<>();

    @Getter
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    private KafkaSnapshotPublishedOffsetsRepository kafkaSnapshotPublishedOffsetsRepository;

    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    protected AbstractEntitiesUploader() {
        this.changesMap = new HashMap<>();
        this.uploadingEntityDescriptors = new ArrayList<>();
    }
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="API">

    /**
     * Если описателя с таким топиком еще нет, то добавляется. Если есть, то заменяется.
     *
     * @param descriptor Описатель, который надо зарегистрировать.
     * @return this.
     */
    @Override
    @NotNull
    public <O extends DataObject, P extends DataPackage<O>, E extends EntityObject>
    AbstractEntitiesUploader register(@NotNull final UploadingEntityDescriptor<O, P, E> descriptor) {
        final var descriptors = this.uploadingEntityDescriptors;
        for (int i = 0; i < descriptors.size(); i++) {
            final var local = descriptors.get(i);
            if (local.getTopic().equals(descriptor.getTopic())) {
                descriptors.set(i, (UploadingEntityDescriptor<? extends DataObject, ? extends DataPackage<DataObject>, ? extends EntityObject>) descriptor);
                return this;
            }
        }
        descriptors.add((UploadingEntityDescriptor<? extends DataObject, ? extends DataPackage<DataObject>, ? extends EntityObject>) descriptor);
        return this;
    }

    /**
     * @param topic Топик, для которого требуется найти описатель выгрузки.
     * @return Получение описателя с указанным топиком. Если такого описателя нет, то возвращается null.
     */
    @SuppressWarnings("unchecked")
    @Override
    @NotNull
    public <O extends DataObject, P extends DataPackage<O>, E extends EntityObject>
    UploadingEntityDescriptor<O, P, E> getDescriptor(@NotNull final String topic) {
        for (var descriptor : getAllDescriptors()) {
            if (descriptor.getTopic().equals(topic)) {
                return (UploadingEntityDescriptor<O, P, E>) descriptor;
            }
        }
        throw new InvalidParameterException("Not registered descriptor for topic " + topic);
    }

    /**
     * @param entityClass Класс сущности, для которого требуется найти описатель выгрузки.
     * @return Получение описателя с указанным классом сущности. Если такого описателя нет, то возвращается null.
     */
    @SuppressWarnings("unchecked")
    @Override
    @NotNull
    public <O extends DataObject, P extends DataPackage<O>, E extends EntityObject>
    UploadingEntityDescriptor<O, P, E> getDescriptor(@NotNull final Class<? extends EntityObject> entityClass) {
        for (var descriptor : getAllDescriptors()) {
            if (descriptor.getEntityClass() == entityClass) {
                return (UploadingEntityDescriptor<O, P, E>) descriptor;
            }
        }
        throw new InvalidParameterException("Not registered descriptor for entityClass " + entityClass.getSimpleName());
    }

    /**
     * @return Список всех описателей выгрузки.
     */
    @Override
    @NotNull
    public Collection<UploadingEntityDescriptor<? extends DataObject, ? extends DataPackage<DataObject>, ? extends EntityObject>>
    getAllDescriptors() {
        return this.uploadingEntityDescriptors;
    }

    @NotNull
    protected Collection<EntityObject> getChangesList(@NotNull final Class<? extends EntityObject> entityClass) {
        return this.changesMap.computeIfAbsent(entityClass, k -> new ArrayList<>());
    }

    /**
     * Фиксация изменений у сущностей указанного класса.
     *
     * @param entityClass     Класс сущности изменившихся экземпляров.
     * @param changedEntities Список изменившихся экземпляров сущности.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    @Override
    public void setChanges(@NotNull final Class<? extends EntityObject> entityClass, @NotNull final Collection<EntityObject> changedEntities) {
        synchronized (entityClass) {
            final var list = getChangesList(entityClass);
            changedEntities.forEach(e -> {
                if (!list.contains(e)) {
                    list.add(e);
                }
            });
        }
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    @Override
    @NotNull
    public PartitionOffset uploadChangesByEntityClass(
            @NotNull final Class<? extends EntityObject> entityClass
    ) throws Exception {
        synchronized (entityClass) {
            // Получаем описатель выгрузки для данной сущности
            final var uploadDescriptor = getDescriptor(entityClass);
            // Получаем список изменений
            var entityObjects = getChangesList(entityClass);
            // Конвертируем список Entity -> DTO-пакет
            final var dtoPackage = this.simpleOutcomeTopicUploader.createPackage(uploadDescriptor);
            uploadDescriptor.getConverter().fillDtoPackageFromEntitiesPackage(dtoPackage, entityObjects);
            // Выгружаем в Kafka
            final var result = this.simpleOutcomeTopicUploader.uploadDataPackage(uploadDescriptor, dtoPackage);
            // Чистим список изменений
            entityObjects.clear();
            return result;
        }
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    @Override
    @NotNull
    public PartitionOffset uploadSnapshot(
            @NotNull final Class<? extends EntityObject> entityClass,
            @NotNull final Collection<EntityObject> allObjects
    ) throws Exception {
        synchronized (entityClass) {
            // Получаем описатель выгрузки для данной сущности
            final var uploadDescriptor = getDescriptor(entityClass);
            // Конвертируем список Entity -> DTO-пакет
            final var dtoPackage = createAndFillDtoPackage(uploadDescriptor, allObjects);
            // Обновляем memoryRepository
            internalLoadToMemoryRepository(uploadDescriptor, dtoPackage);
            // Выгружаем в Kafka
            final var result = this.simpleOutcomeTopicUploader.uploadDataPackage(uploadDescriptor, dtoPackage);
            // Чистим список изменений
            getChangesList(entityClass).clear();
            return result;
        }
    }

    @Override
    public void publishSnapshot(@NotNull String topic) throws Exception {
        final var descriptor = getDescriptor(topic);
        log.info("Starting loading dictionary {}", descriptor.getEntityClass().getSimpleName());
        final var allObjects = descriptor.getRepository().findAll();
        log.info("Starting publish dictionary {} into topic {}; object count {}", descriptor.getEntityClass().getSimpleName(), descriptor.getTopic(), allObjects.size());
        final var partitionOffset = uploadSnapshot(descriptor.getEntityClass(), allObjects);

        final var kafkaSnapshotPublishedOffsetEntity = new KafkaSnapshotPublishedOffsetEntity()
                .setTopic(descriptor.getTopic())
                .setPartition(partitionOffset.getPartition())
                .setStartOffset(partitionOffset.getOffset());
        this.kafkaSnapshotPublishedOffsetEntitiesCache.put(descriptor.getTopic(), kafkaSnapshotPublishedOffsetEntity);
        this.kafkaSnapshotPublishedOffsetsRepository.save(kafkaSnapshotPublishedOffsetEntity);
        log.info("Dictionary {} published into topic {}. Partition: {}, offset: {}", descriptor.getEntityClass().getSimpleName(), descriptor.getTopic(), partitionOffset.getPartition(), partitionOffset.getOffset());
    }

    @Override
    @Nullable
    public KafkaSnapshotPublishedOffsetEntity getOffset(@NotNull final String topic) {
        return this.kafkaSnapshotPublishedOffsetEntitiesCache.get(topic);
    }
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Internal methods">
    @NotNull
    protected <O extends DataObject, P extends DataPackage<O>, E extends EntityObject>
    P createAndFillDtoPackage(
            @NotNull final UploadingEntityDescriptor<O, P, E> uploadDescriptor,
            @NotNull final Collection<E> allObjects
    ) throws Exception {
        final var dtoPackage = this.simpleOutcomeTopicUploader.createPackage(uploadDescriptor);
        uploadDescriptor.getConverter().fillDtoPackageFromEntitiesPackage(dtoPackage, allObjects);
        return dtoPackage;
    }

    protected <O extends DataObject, P extends DataPackage<O>, E extends EntityObject>
    void internalLoadToMemoryRepository(
            @NotNull final UploadingEntityDescriptor<O, P, E> uploadDescriptor,
            @NotNull final P dtoPackage
    ) throws JsonMappingException, ObjectNotExistsException, ObjectAlreadyExistsException {
        final var memRepo = uploadDescriptor.getMemoryRepository();
        if (memRepo == null) {
            return;
        }

        for(var dataObject: dtoPackage.getObjects()) {
            final var key = memRepo.extractKey(dataObject);
            if (memRepo.containsKey(key)) {
                memRepo.update(dataObject);
            } else {
                memRepo.insert(dataObject);
            }
        }
    }
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
}
