package com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.MaintenanceOperationConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;

public class CleanupByUploadWrapperConfiguration extends BaseWrapperStepConfiguration {

    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("maintenance_configuration")
    private MaintenanceOperationConfiguration maintenanceOperationConfiguration;

    public MaintenanceOperationConfiguration getMaintenanceOperationConfiguration() {
        return maintenanceOperationConfiguration;
    }

    public void setMaintenanceOperationConfiguration(MaintenanceOperationConfiguration maintenanceOperationConfiguration) {
        this.maintenanceOperationConfiguration = maintenanceOperationConfiguration;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
