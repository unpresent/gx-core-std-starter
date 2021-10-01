package ru.gx.std.upload;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;
import ru.gx.data.jpa.EntityObject;
import ru.gx.kafka.PartitionOffset;
import ru.gx.std.entities.KafkaSnapshotPublishedOffsetEntity;

import java.util.Collection;

public interface EntitiesUploader {
    @NotNull
    <O extends DataObject, P extends DataPackage<O>, E extends EntityObject>
    AbstractEntitiesUploader register(@NotNull final UploadingEntityDescriptor<O, P, E> descriptor);

    @NotNull
    <O extends DataObject, P extends DataPackage<O>, E extends EntityObject>
    UploadingEntityDescriptor<O, P, E> getDescriptor(@NotNull final String topic);

    @NotNull
    <O extends DataObject, P extends DataPackage<O>, E extends EntityObject>
    UploadingEntityDescriptor<O, P, E> getDescriptor(@NotNull final Class<? extends EntityObject> entityClass);

    @NotNull
    Collection<UploadingEntityDescriptor<? extends DataObject, ? extends DataPackage<DataObject>, ? extends EntityObject>>
    getAllDescriptors();

    void setChanges(@NotNull final Class<? extends EntityObject> entityClass, @NotNull final Collection<EntityObject> changedEntities);

    @NotNull
    PartitionOffset uploadChangesByEntityClass(@NotNull final Class<? extends EntityObject> entityClass) throws Exception;

    @NotNull
    PartitionOffset uploadSnapshot(
            @NotNull final Class<? extends EntityObject> entityClass,
            @NotNull final Collection<EntityObject> allObjects
    ) throws Exception;

    void publishSnapshot(@NotNull String topic) throws Exception;

    @Nullable
    KafkaSnapshotPublishedOffsetEntity getOffset(@NotNull final String topic);
}
