package com.latticeengines.apps.cdl.workflow;

import com.latticeengines.apps.cdl.service.impl.CDLDataCleanupServiceImpl;
import com.latticeengines.domain.exposed.cdl.CleanupByUploadConfiguration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupOperationConfiguration;
import com.latticeengines.domain.exposed.cdl.MaintenanceOperationConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.CDLOperationWorkflowConfiguration;

@Component
public class CDLOperationWorkflowSubmitter extends WorkflowSubmitter {
    private static final Logger log = LoggerFactory.getLogger(CDLOperationWorkflowSubmitter.class);

    public ApplicationId submit(CustomerSpace customerSpace,
                                MaintenanceOperationConfiguration maintenanceOperationConfiguration) {
        log.info("generate Configuration");
        CDLOperationWorkflowConfiguration configuration = generateConfiguration(customerSpace,
                maintenanceOperationConfiguration);

        log.info("submit Configuration");
        return workflowJobService.submit(configuration);
    }

    private CDLOperationWorkflowConfiguration generateConfiguration(CustomerSpace customerSpace,
                                            MaintenanceOperationConfiguration maintenanceOperationConfiguration) {
        boolean isCleanupByUpload = false;
        if(maintenanceOperationConfiguration instanceof CleanupOperationConfiguration) {
            log.info("Configuratin is CleanupOperation");
            isCleanupByUpload = ((CleanupOperationConfiguration) maintenanceOperationConfiguration)
                    .getCleanupOperationType().isNeedTransFlow();
        }
        String filePath = "";
        String tableName = "";
        if(maintenanceOperationConfiguration instanceof CleanupByUploadConfiguration) {
            log.info("Configuratin is CleanupByUpload");
            CleanupByUploadConfiguration cleanupByUploadConfiguration = ((CleanupByUploadConfiguration) maintenanceOperationConfiguration);
            filePath = cleanupByUploadConfiguration.getFilePath();
            tableName = cleanupByUploadConfiguration.getTableName();
        }
        return new CDLOperationWorkflowConfiguration.Builder()
                .customer(customerSpace)
                .internalResourceHostPort(internalResourceHostPort)
                .microServiceHostPort(microserviceHostPort)
                .maintenanceOperationConfiguration(maintenanceOperationConfiguration)
                .isCleanupByUpload(isCleanupByUpload)
                .filePath(filePath)
                .tableName(tableName)
                .build();
    }
}
