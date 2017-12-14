package com.latticeengines.cdl.workflow.steps.maintenance;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.StartMaintenanceConfiguration;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("startMaintenanceStep")
public class StartMaintenanceStep extends BaseWorkflowStep<StartMaintenanceConfiguration> {
    @Override
    public void execute() {

    }
}
