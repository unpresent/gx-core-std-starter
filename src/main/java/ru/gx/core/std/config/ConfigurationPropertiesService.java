package ru.gx.core.std.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import ru.gx.core.worker.CommonWorkerSettingsDefaults;

@ConfigurationProperties(prefix = "service.kafka")
@Getter
@Setter
public class ConfigurationPropertiesService {
    @NestedConfigurationProperty
    private OffsetsController offsetsController;

    @NestedConfigurationProperty
    public DbSaver dbSaver;

    @Getter
    @Setter
    public static class OffsetsController {
        public OffsetsControllerType type = OffsetsControllerType.File;
        public String fileStorage = "offsets.data";
    }

    public enum OffsetsControllerType {
        File,
        Sql
    }

    @Getter
    @Setter
    public static class DbSaver {
        public static final int SETTING_ACCUMULATE_DURING_MS_DEFAULT = 1000;
        public static final int SETTING_PACKAGE_LIMIT_SIZE_DEFAULT = 100;

        public boolean enabled = false;

        public boolean enabledTransaction = true;

        private int waitOnStopMs = CommonWorkerSettingsDefaults.WAIT_ON_STOP_MS_DEFAULT;
        private int waitOnRestartMs = CommonWorkerSettingsDefaults.WAIT_ON_RESTART_MS_DEFAULT;
        private int minTimePerIterationMs = CommonWorkerSettingsDefaults.MIN_TIME_PER_ITERATION_MS_DEFAULT;
        private int timeoutRunnerLifeMs = CommonWorkerSettingsDefaults.TIMEOUT_RUNNER_LIFE_MS_DEFAULT;
        private int printStatisticsEveryMs = CommonWorkerSettingsDefaults.PRINT_STATISTICS_EVERY_MS_DEFAULT;

        public int accumulateDuringMs = SETTING_ACCUMULATE_DURING_MS_DEFAULT;
        public int packageLimitSize = SETTING_PACKAGE_LIMIT_SIZE_DEFAULT;
    }
}
