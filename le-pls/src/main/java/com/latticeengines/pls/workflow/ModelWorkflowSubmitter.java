package com.latticeengines.pls.workflow;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.leadprioritization.workflow.ModelWorkflowConfiguration;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public class ModelWorkflowSubmitter extends BaseModelWorkflowSubmitter {
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ImportMatchAndModelWorkflowSubmitter.class);

    public ApplicationId submit(String eventTableName, String modelName, String displayName,
            String sourceSchemaInterpretation, String trainingTableName) {
        Table eventTable = metadataProxy.getTable(MultiTenantContext.getCustomerSpace().toString(), eventTableName);

        if (eventTable == null) {
            throw new LedpException(LedpCode.LEDP_18088, new String[] { eventTableName });
        }

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "modelAndEmailWorkflow");

        ModelWorkflowConfiguration configuration = new ModelWorkflowConfiguration.Builder()
                .microServiceHostPort(microserviceHostPort) //
                .customer(getCustomerSpace()) //
                .workflow("modelWorkflow") //
                .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .modelName(modelName) //
                .displayName(displayName) //
                .eventTableName(eventTableName) //
                .internalResourceHostPort(internalResourceHostPort) //
                .sourceSchemaInterpretation(sourceSchemaInterpretation) //
                .inputProperties(inputProperties) //
                .trainingTableName(trainingTableName) //
                .build();
        return workflowJobService.submit(configuration);
    }

}
