package com.latticeengines.cdl.workflow.steps.maintenance;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.OperationExecuteConfiguration;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.cdl.CDLMaintenanceProxy;
import com.latticeengines.serviceflows.workflow.report.BaseReportStep;

@Component("operationExecuteStep")
public class OperationExecuteStep extends BaseReportStep<OperationExecuteConfiguration> {

    @Autowired
    private CDLMaintenanceProxy cdlMaintenanceProxy;

    @Override
    public void execute() {
        //
        cdlMaintenanceProxy.maintenance(configuration.getCustomerSpace().toString(), configuration
                .getMaintenanceOperationConfiguration());
        super.execute();
    }


    @Override
    protected ReportPurpose getPurpose() {
        return ReportPurpose.MAINTENANCE_OPERATION_SUMMARY;
    }
}
