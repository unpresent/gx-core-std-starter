package ru.gx.std.upload;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.springframework.data.jpa.repository.JpaRepository;
import ru.gx.data.DataMemoryRepository;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;
import ru.gx.data.jpa.AbstractDtoFromEntityConverter;
import ru.gx.data.jpa.EntityObject;
import ru.gx.kafka.upload.OutcomeTopicUploadingDescriptor;
import ru.gx.kafka.upload.OutcomeTopicUploadingDescriptorsDefaults;

import java.lang.reflect.ParameterizedType;

@Accessors(chain = true)
@EqualsAndHashCode(callSuper = true)
@ToString
public class UploadingEntityDescriptor<O extends DataObject, P extends DataPackage<O>, E extends EntityObject>
        extends OutcomeTopicUploadingDescriptor<O, P> {

    @Getter
    @Setter
    private Class<? extends EntityObject> entityClass;

    @Getter
    @Setter
    private AbstractDtoFromEntityConverter<O, P, E> converter;

    @Getter
    @Setter
    private JpaRepository<E, ?> repository;

    @Getter
    @Setter
    private DataMemoryRepository<O, P> memoryRepository;

    @SuppressWarnings("unchecked")
    public UploadingEntityDescriptor(String topic, OutcomeTopicUploadingDescriptorsDefaults defaults) {
        super(topic, defaults);

        final var thisClass = this.getClass();
        final var superClass = thisClass.getGenericSuperclass();
        if (superClass instanceof ParameterizedType) {
            this.entityClass = (Class<E>) ((ParameterizedType) superClass).getActualTypeArguments()[2];
        }
    }
}
