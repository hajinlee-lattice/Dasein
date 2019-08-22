package com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class EntityMatchMigrateStepConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("migrate_tracking_pid")
    private Long migrateTrackingPid;

    @JsonProperty("datafeedtask_map")
    private Map<BusinessEntity, List<String>> dataFeedTaskMap;

    public Long getMigrateTrackingPid() {
        return migrateTrackingPid;
    }

    public void setMigrateTrackingPid(Long migrateTrackingPid) {
        this.migrateTrackingPid = migrateTrackingPid;
    }

    public Map<BusinessEntity, List<String>> getDataFeedTaskMap() {
        return dataFeedTaskMap;
    }

    public void setDataFeedTaskMap(Map<BusinessEntity, List<String>> dataFeedTaskMap) {
        this.dataFeedTaskMap = dataFeedTaskMap;
    }
}
