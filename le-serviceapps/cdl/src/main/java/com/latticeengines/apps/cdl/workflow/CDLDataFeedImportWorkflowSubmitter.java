package com.latticeengines.apps.cdl.workflow;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.latticeeingines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.serviceflows.cdl.CDLDataFeedImportWorkflowConfiguration;

@Component
public class CDLDataFeedImportWorkflowSubmitter extends WorkflowSubmitter {

    public ApplicationId submit(CustomerSpace customerSpace, DataFeedTask dataFeedTask, String connectorConfig) {
        CDLDataFeedImportWorkflowConfiguration configuration = generateConfiguration(customerSpace, dataFeedTask, connectorConfig);

        return workflowJobService.submit(configuration);
    }

    private CDLDataFeedImportWorkflowConfiguration generateConfiguration(CustomerSpace customerSpace,
            DataFeedTask dataFeedTask, String connectorConfig) {

        return new CDLDataFeedImportWorkflowConfiguration.Builder()
                .customer(customerSpace)
                .microServiceHostPort(microserviceHostPort)
                .dataFeedTaskId(dataFeedTask.getUniqueId())
                .importConfig(connectorConfig)
                .build();
    }
}
