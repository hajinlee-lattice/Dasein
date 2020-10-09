package com.latticeengines.apps.cdl.workflow;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.domain.exposed.serviceflows.cdl.GenerateIntentEmailAlertWorkflowConfiguration;

@Component
public class GenerateIntentAlertWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(GenerateIntentAlertWorkflowSubmitter.class);

    @WithWorkflowJobPid
    public ApplicationId submit() {

        GenerateIntentEmailAlertWorkflowConfiguration config = new GenerateIntentEmailAlertWorkflowConfiguration.Builder()
                .customer(getCustomerSpace()) //
                .workflow(GenerateIntentEmailAlertWorkflowConfiguration.WORKFLOW_NAME) //
                .internalResourceHostPort(internalResourceHostPort) //
                .build();

        return workflowJobService.submit(config);
    }
}
