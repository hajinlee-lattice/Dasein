package com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.core.steps.BaseReportStepConfiguration;

public class InputFileValidatorConfiguration extends BaseReportStepConfiguration {
    @JsonProperty("entity")
    private BusinessEntity entity;

    @JsonProperty("enable_entity_match")
    private boolean enableEntityMatch;

    @JsonProperty("enable_entity_match_ga")
    private boolean enableEntityMatchGA;

    @JsonProperty("data_feed_task_id")
    private String dataFeedTaskId;

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    public boolean isEnableEntityMatch() {
        return enableEntityMatch;
    }

    public void setEnableEntityMatch(boolean enableEntityMatch) {
        this.enableEntityMatch = enableEntityMatch;
    }

    public boolean isEnableEntityMatchGA() {
        return enableEntityMatchGA;
    }

    public void setEnableEntityMatchGA(boolean enableEntityMatchGA) {
        this.enableEntityMatchGA = enableEntityMatchGA;
    }

    public String getDataFeedTaskId() {
        return dataFeedTaskId;
    }

    public void setDataFeedTaskId(String dataFeedTaskId) {
        this.dataFeedTaskId = dataFeedTaskId;
    }
}
