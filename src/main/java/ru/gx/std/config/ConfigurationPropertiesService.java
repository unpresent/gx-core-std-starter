package ru.gx.std.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

@ConfigurationProperties(prefix = "service")
@Getter
@Setter
public class ConfigurationPropertiesService {
    @NestedConfigurationProperty
    private OffsetsLoaders offsetsLoaders;

    @NestedConfigurationProperty
    private OffsetsSavers offsetsSavers;

    @Getter
    @Setter
    public static class OffsetsLoaders {
        @NestedConfigurationProperty
        private NativeJdbc nativeJdbc;

        @NestedConfigurationProperty
        private Jpa jpa;
    }

    @Getter
    @Setter
    public static class OffsetsSavers {
        @NestedConfigurationProperty
        private NativeJdbc nativeJdbc;

        @NestedConfigurationProperty
        private Jpa jpa;
    }

    @Getter
    @Setter
    public static class NativeJdbc {
        private boolean enabled = true;
    }

    @Getter
    @Setter
    public static class Jpa {
        private boolean enabled = true;
    }
}
