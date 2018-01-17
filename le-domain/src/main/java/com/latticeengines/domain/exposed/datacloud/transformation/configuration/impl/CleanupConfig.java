package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class CleanupConfig extends TransformerConfig {


    @JsonProperty("operation_type")
    private CleanupOperationType operationType;

    @JsonProperty("business_entity")
    private BusinessEntity businessEntity;

    @JsonProperty("join_column")
    private String joinColumn;

    public CleanupOperationType getOperationType() {
        return operationType;
    }

    public void setOperationType(CleanupOperationType operationType) {
        this.operationType = operationType;
    }

    public BusinessEntity getBusinessEntity() {
        return businessEntity;
    }

    public void setBusinessEntity(BusinessEntity businessEntity) {
        this.businessEntity = businessEntity;
    }

    public String getJoinColumn() {
        return joinColumn;
    }

    public void setJoinColumn(String joinColumn) {
        this.joinColumn = joinColumn;
    }

}