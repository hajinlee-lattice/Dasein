package com.latticeengines.apps.cdl.workflow;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.integration.BrokerInitialLoadRequest;
import com.latticeengines.domain.exposed.serviceflows.cdl.BrokerInitialLoadWorkflowConfiguration;

@Component
public class BrokerInitialLoadWorkflowSubmitter extends WorkflowSubmitter {

    @WithWorkflowJobPid
    public ApplicationId submit(@NotNull CustomerSpace customerSpace, @NotNull BrokerInitialLoadRequest request,
                                @NotNull WorkflowPidWrapper pidWrapper) {
        BrokerInitialLoadWorkflowConfiguration configuration = configure(customerSpace, request);
        return workflowJobService.submit(configuration, pidWrapper.getPid());
    }

    private BrokerInitialLoadWorkflowConfiguration configure(CustomerSpace customerSpace, BrokerInitialLoadRequest request) {
        return new BrokerInitialLoadWorkflowConfiguration.Builder().customer(customerSpace).sourceId(request.getSourceId())
                .startTime(request.getStartTime()).endTime(request.getEndTime()).inboundConnectionType(request.getInboundConnectionType()).build();
    }
}
