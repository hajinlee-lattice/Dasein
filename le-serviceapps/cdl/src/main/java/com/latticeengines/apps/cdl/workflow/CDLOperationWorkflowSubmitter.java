package com.latticeengines.apps.cdl.workflow;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.MaintenanceOperationConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.CDLOperationWorkflowConfiguration;

@Component
public class CDLOperationWorkflowSubmitter extends WorkflowSubmitter {

    public ApplicationId submit(CustomerSpace customerSpace,
                                MaintenanceOperationConfiguration maintenanceOperationConfiguration) {
        CDLOperationWorkflowConfiguration configuration = generateConfiguration(customerSpace,
                maintenanceOperationConfiguration);
        return workflowJobService.submit(configuration);
    }

    private CDLOperationWorkflowConfiguration generateConfiguration(CustomerSpace customerSpace,
                                            MaintenanceOperationConfiguration maintenanceOperationConfiguration) {
        CDLOperationWorkflowConfiguration cdlOperationWorkflowConfiguration = new CDLOperationWorkflowConfiguration();
        return new CDLOperationWorkflowConfiguration.Builder()
                .customer(customerSpace)
                .internalResourceHostPort(internalResourceHostPort)
                .microServiceHostPort(microserviceHostPort)
                .maintenanceOperationConfiguration(maintenanceOperationConfiguration)
                .build();
    }
}
