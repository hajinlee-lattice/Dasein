package com.latticeengines.pls.workflow;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.leadprioritization.workflow.ModelWorkflowConfiguration;
import com.latticeengines.security.exposed.util.SecurityContextUtils;

@Component
public class ModelWorkflowSubmitter extends BaseModelWorkflowSubmitter {
    private static final Logger log = Logger.getLogger(CreateModelWorkflowSubmitter.class);

    public ApplicationId submit(String eventTableName, String modelName) {
        Table eventTable = metadataProxy.getTable(SecurityContextUtils.getCustomerSpace().toString(), eventTableName);

        if (eventTable == null) {
            throw new LedpException(LedpCode.LEDP_18088, new String[] { eventTableName });
        }

        ModelWorkflowConfiguration configuration = new ModelWorkflowConfiguration.Builder()
                .microServiceHostPort(microserviceHostPort) //
                .customer(getCustomerSpace()) //
                .workflow("modelAndEmailWorkflow") //
                .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .modelName(modelName) //
                .eventTableName(eventTableName) //
                .build();
        AppSubmission submission = workflowProxy.submitWorkflowExecution(configuration);
        String applicationId = submission.getApplicationIds().get(0);
        return YarnUtils.appIdFromString(applicationId);
    }

}
