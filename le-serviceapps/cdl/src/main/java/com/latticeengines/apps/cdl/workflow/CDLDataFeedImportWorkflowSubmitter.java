package com.latticeengines.apps.cdl.workflow;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.serviceflows.cdl.CDLDataFeedImportWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;

@Component
public class CDLDataFeedImportWorkflowSubmitter extends WorkflowSubmitter {

    public ApplicationId submit(CustomerSpace customerSpace, DataFeedTask dataFeedTask, String connectorConfig,
            CSVImportFileInfo csvImportFileInfo) {
        CDLDataFeedImportWorkflowConfiguration configuration = generateConfiguration(customerSpace, dataFeedTask,
                connectorConfig, csvImportFileInfo);

        return workflowJobService.submit(configuration);
    }

    private CDLDataFeedImportWorkflowConfiguration generateConfiguration(CustomerSpace customerSpace,
            DataFeedTask dataFeedTask, String connectorConfig, CSVImportFileInfo csvImportFileInfo) {

        return new CDLDataFeedImportWorkflowConfiguration.Builder() //
                .customer(customerSpace) //
                .internalResourceHostPort(internalResourceHostPort) //
                .microServiceHostPort(microserviceHostPort) //
                .dataFeedTaskId(dataFeedTask.getUniqueId()) //
                .importConfig(connectorConfig) //
                .inputProperties(ImmutableMap.<String, String> builder()
                        .put(WorkflowContextConstants.Inputs.DATAFEEDTASK_IMPORT_IDENTIFIER, dataFeedTask.getUniqueId()) //
                        .put(WorkflowContextConstants.Inputs.SOURCE_FILE_NAME, csvImportFileInfo.getReportFileName()) //
                        .put(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME,
                                csvImportFileInfo.getReportFileDisplayName()) //
                        .build())
                .build();
    }
}
