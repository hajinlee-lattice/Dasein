package com.latticeengines.domain.exposed.datacloud.orchestration;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

/**
 * Keep doc
 * https://confluence.lattice-engines.com/display/ENG/DataCloud+Engine+Architecture#DataCloudEngineArchitecture-Orchestration
 * up to date if there is any new change
 *
 * Trigger strategy: Trigger when BOTH current time satisfies defined cron
 * expression and there is a new version of finished predefined engine job
 *
 * Version strategy: Use latest version of finished predefined engine job
 * (ingested source version / transformation pipeline version / published source
 * version)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ExternalTriggerWithScheduleConfig extends OrchestrationConfig {

    @JsonProperty("ScheduleConfig")
    private PredefinedScheduleConfig scheduleConfig;

    @JsonProperty("ExternalTriggerConfig")
    private ExternalTriggerConfig externalTriggerConfig;

    public PredefinedScheduleConfig getScheduleConfig() {
        return scheduleConfig;
    }

    public void setScheduleConfig(PredefinedScheduleConfig scheduleConfig) {
        this.scheduleConfig = scheduleConfig;
    }

    public ExternalTriggerConfig getExternalTriggerConfig() {
        return externalTriggerConfig;
    }

    public void setExternalTriggerConfig(ExternalTriggerConfig externalTriggerConfig) {
        this.externalTriggerConfig = externalTriggerConfig;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
