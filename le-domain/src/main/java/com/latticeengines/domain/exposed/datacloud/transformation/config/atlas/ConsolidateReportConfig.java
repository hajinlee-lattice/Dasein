package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ConsolidateReportConfig extends TransformerConfig {

    @JsonProperty("BusinessEntity")
    private BusinessEntity entity;

    // threshold timestamp for determine new records
    // if not specified use pipeline timestamp
    @JsonProperty("ThresholdTime")
    private Long thresholdTime;

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    public Long getThresholdTime() {
        return thresholdTime;
    }

    public void setThresholdTime(Long thresholdTime) {
        this.thresholdTime = thresholdTime;
    }
}
