package com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class ProductFileConfiguration extends MicroserviceStepConfiguration {
    @JsonProperty("data_feed_task_id")
    private String dataFeedTaskId;

    public String getDataFeedTaskId() {
        return dataFeedTaskId;
    }

    public void setDataFeedTaskId(String dataFeedTaskId) {
        this.dataFeedTaskId = dataFeedTaskId;
    }
}
