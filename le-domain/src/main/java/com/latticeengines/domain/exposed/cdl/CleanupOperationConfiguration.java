package com.latticeengines.domain.exposed.cdl;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({ @JsonSubTypes.Type(value = CleanupAllConfiguration.class, name = "CleanupAllConfiguration"),
        @JsonSubTypes.Type(value = CleanupByDateRangeConfiguration.class, name = "CleanupByDateRangeConfiguration"),
        @JsonSubTypes.Type(value = CleanupByUploadConfiguration.class, name = "CleanupByUploadConfiguration") })
public class CleanupOperationConfiguration extends MaintenanceOperationConfiguration {

    @JsonProperty("entity")
    private BusinessEntity entity;

    @NotNull
    @JsonProperty("cleanup_operation_type")
    private CleanupOperationType cleanupOperationType;

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    public CleanupOperationType getCleanupOperationType() {
        return cleanupOperationType;
    }

    public void setCleanupOperationType(CleanupOperationType cleanupOperationType) {
        this.cleanupOperationType = cleanupOperationType;
    }
}
