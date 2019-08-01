package com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class RegisterImportActionStepConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("business_entity")
    private BusinessEntity entity;

    @JsonProperty("action_pid")
    private Long actionPid;

    @JsonProperty("migrate_tracking_pid")
    private Long migrateTrackingPid;

    @JsonProperty("convert_service_config")
    private BaseConvertBatchStoreServiceConfiguration convertServiceConfig;

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    public Long getActionPid() {
        return actionPid;
    }

    public void setActionPid(Long actionPid) {
        this.actionPid = actionPid;
    }

    public Long getMigrateTrackingPid() {
        return migrateTrackingPid;
    }

    public void setMigrateTrackingPid(Long migrateTrackingPid) {
        this.migrateTrackingPid = migrateTrackingPid;
    }

    public BaseConvertBatchStoreServiceConfiguration getConvertServiceConfig() {
        return convertServiceConfig;
    }

    public void setConvertServiceConfig(BaseConvertBatchStoreServiceConfiguration convertServiceConfig) {
        this.convertServiceConfig = convertServiceConfig;
    }
}
