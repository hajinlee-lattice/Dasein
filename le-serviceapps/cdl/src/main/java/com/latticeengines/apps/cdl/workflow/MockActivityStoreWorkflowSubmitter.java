package com.latticeengines.apps.cdl.workflow;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.MockActivityStoreWorkflowConfiguration;


@Component
public class MockActivityStoreWorkflowSubmitter extends WorkflowSubmitter {

    @Value("${eai.export.dynamo.signature}")
    private String signature;

    @WithWorkflowJobPid
    public ApplicationId submit(@NotNull CustomerSpace customerSpace, @NotNull WorkflowPidWrapper pidWrapper) {
        MockActivityStoreWorkflowConfiguration configuration = configure(customerSpace);
        return workflowJobService.submit(configuration, pidWrapper.getPid());
    }

    private MockActivityStoreWorkflowConfiguration configure(CustomerSpace customerSpace) {
        return new MockActivityStoreWorkflowConfiguration.Builder() //
                .customer(customerSpace) //
                .dynamoSignature(signature) //
                .build();
    }

}
