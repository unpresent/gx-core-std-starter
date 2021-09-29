package ru.gx.kafka.upload;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.gx.data.*;
import ru.gx.data.jpa.EntityObject;
import ru.gx.kafka.PartitionOffset;

import java.security.InvalidParameterException;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
@SuppressWarnings("unused")
public class EntitiesUploader {
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    @Getter(AccessLevel.PROTECTED)
    @NotNull
    private final Map<Class<? extends EntityObject>, Collection<EntityObject>> changesMap;

    @NotNull
    private final List<UploadingEntityDescriptor<? extends DataObject, ? extends DataPackage<DataObject>, ? extends EntityObject>> uploadingEntityDescriptors;

    @Getter
    @NotNull
    private final SimpleOutcomeTopicUploader simpleOutcomeTopicUploader;

    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    public EntitiesUploader(@NotNull final ObjectMapper objectMapper) {
        this.simpleOutcomeTopicUploader = new SimpleOutcomeTopicUploader(objectMapper);
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
    @NotNull
    public <O extends DataObject, P extends DataPackage<O>, E extends EntityObject>
    EntitiesUploader register(@NotNull final UploadingEntityDescriptor<O, P, E> descriptor) {
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
