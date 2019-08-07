package com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate;


import com.fasterxml.jackson.annotation.JsonProperty;

public class MigrateImportServiceConfiguration extends BaseConvertBatchStoreServiceConfiguration {

    @JsonProperty("migrate_tracking_pid")
    private Long migrateTrackingPid;

    public Long getMigrateTrackingPid() {
        return migrateTrackingPid;
    }

    public void setMigrateTrackingPid(Long migrateTrackingPid) {
        this.migrateTrackingPid = migrateTrackingPid;
    }
}
