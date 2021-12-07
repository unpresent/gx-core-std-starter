package ru.gx.core.std.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

@ConfigurationProperties(prefix = "service")
@Getter
@Setter
public class ConfigurationPropertiesService {
    @NestedConfigurationProperty
    private OffsetsController offsetsController;

    @Getter
    @Setter
    public static class OffsetsController {
        public OffsetsControllerType type = OffsetsControllerType.File;
        public String fileStorage = "offsets.data";
    }

    public enum OffsetsControllerType {
        File,
        Jdbc,
        Jpa
    }
}
