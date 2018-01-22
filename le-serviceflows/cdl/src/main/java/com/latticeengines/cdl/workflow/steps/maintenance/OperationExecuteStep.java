package com.latticeengines.cdl.workflow.steps.maintenance;

import java.util.Map;
import java.util.Optional;

import org.springframework.stereotype.Component;

import com.latticeengines.cdl.operationflow.service.MaintenanceOperationService;
import com.latticeengines.domain.exposed.cdl.MaintenanceOperationConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.OperationExecuteConfiguration;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.serviceflows.workflow.report.BaseReportStep;

@Component("operationExecuteStep")
public class OperationExecuteStep extends BaseReportStep<OperationExecuteConfiguration> {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void execute() {
        //
        MaintenanceOperationConfiguration maintenanceOperationConfiguration = configuration.getMaintenanceOperationConfiguration();
        MaintenanceOperationService maintenanceOperationService = MaintenanceOperationService.getMaintenanceService
                (maintenanceOperationConfiguration.getClass());
        if (maintenanceOperationService == null) {
            throw new RuntimeException(
                    String.format("Cannot find maintenance service for class: %s", maintenanceOperationConfiguration.getClass()));
        }
        Map<String, Long> report = maintenanceOperationService.invoke(maintenanceOperationConfiguration);
        Optional.ofNullable(report).ifPresent(map -> map.forEach((entity, rows) -> {
            getJson().put(entity + "_Deleted", rows);
        }));
        super.execute();
    }


    @Override
    protected ReportPurpose getPurpose() {
        return ReportPurpose.MAINTENANCE_OPERATION_SUMMARY;
    }
}
