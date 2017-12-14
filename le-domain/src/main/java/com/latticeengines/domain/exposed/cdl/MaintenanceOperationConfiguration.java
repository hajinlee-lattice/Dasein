package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({ @JsonSubTypes.Type(value = CleanupOperationConfiguration.class, name = "CleanupOperationConfiguration")
                })
public class MaintenanceOperationConfiguration {

    @JsonProperty("operation_type")
    private MaintenanceOperationType operationType;

    public MaintenanceOperationType getOperationType() {
        return operationType;
    }

    public void setOperationType(MaintenanceOperationType operationType) {
        this.operationType = operationType;
    }
}
