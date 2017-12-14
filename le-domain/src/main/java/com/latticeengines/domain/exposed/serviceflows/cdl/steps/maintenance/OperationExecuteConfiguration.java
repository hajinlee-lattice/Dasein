package com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.MaintenanceOperationConfiguration;
import com.latticeengines.domain.exposed.cdl.MaintenanceOperationType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.core.steps.BaseReportStepConfiguration;

public class OperationExecuteConfiguration extends BaseReportStepConfiguration {

    @JsonProperty("maintenance_configuration")
    private MaintenanceOperationConfiguration maintenanceOperationConfiguration;

    public MaintenanceOperationConfiguration getMaintenanceOperationConfiguration() {
        return maintenanceOperationConfiguration;
    }

    public void setMaintenanceOperationConfiguration(MaintenanceOperationConfiguration maintenanceOperationConfiguration) {
        this.maintenanceOperationConfiguration = maintenanceOperationConfiguration;
    }
}
