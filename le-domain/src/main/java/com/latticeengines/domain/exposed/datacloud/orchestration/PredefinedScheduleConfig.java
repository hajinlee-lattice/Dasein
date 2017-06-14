package com.latticeengines.domain.exposed.datacloud.orchestration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class PredefinedScheduleConfig extends OrchestrationConfig {
    private String cronExpression;

    @JsonProperty("CronExpression")
    public String getCronExpression() {
        return cronExpression;
    }

    @JsonProperty("CronExpression")
    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
