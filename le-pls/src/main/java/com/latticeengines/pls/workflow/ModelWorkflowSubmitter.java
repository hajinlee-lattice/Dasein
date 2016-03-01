package com.latticeengines.pls.workflow;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.leadprioritization.workflow.ModelWorkflowConfiguration;
import com.latticeengines.security.exposed.util.SecurityContextUtils;

@Component
public class ModelWorkflowSubmitter extends BaseModelWorkflowSubmitter {
    private static final Logger log = Logger.getLogger(CreateModelWorkflowSubmitter.class);

    public ApplicationId submit(ModelingParameters parameters) {
        Table eventTable = metadataProxy.getTable(SecurityContextUtils.getCustomerSpace().toString(),
                parameters.getEventTableName());

        if (eventTable == null) {
            throw new LedpException(LedpCode.LEDP_18088, new String[] { parameters.getEventTableName() });
        }

        List<String> eventColumns = getEventColumns(eventTable);
        ModelWorkflowConfiguration configuration = new ModelWorkflowConfiguration.Builder()
                .microServiceHostPort(microserviceHostPort) //
                .customer(getCustomerSpace()) //
                .workflow("modelAndEmailWorkflow") //
                .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .modelName(parameters.getName()) //
                .eventColumns(eventColumns) //
                .eventTableName(parameters.getEventTableName()) //
                .build();
        AppSubmission submission = workflowProxy.submitWorkflowExecution(configuration);
        String applicationId = submission.getApplicationIds().get(0);
        return YarnUtils.appIdFromString(applicationId);
    }

}
