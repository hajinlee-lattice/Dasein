package com.latticeengines.domain.exposed.datacloud.orchestration;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

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
