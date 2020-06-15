package com.latticeengines.apps.cdl.workflow;


import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.service.ZKConfigService;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.AtlasProfileReportRequest;
import com.latticeengines.domain.exposed.serviceflows.cdl.AtlasProfileReportWorkflowConfiguration;

@Component
public class AtlasProfileReportWorkflowSubmitter extends WorkflowSubmitter {

    @Inject
    private ZKConfigService zkConfigService;

    @WithWorkflowJobPid
    public ApplicationId submit(@NotNull String customerSpace,
                                             @NotNull AtlasProfileReportRequest request,
                                             @NotNull WorkflowPidWrapper pidWrapper) {
        boolean internalEnrichEnabled = zkConfigService.isInternalEnrichmentEnabled(CustomerSpace.parse(customerSpace));
        AtlasProfileReportWorkflowConfiguration configuration = new AtlasProfileReportWorkflowConfiguration.Builder() //
                .customer(CustomerSpace.parse(customerSpace)) //
                .allowInternalEnrichAttrs(internalEnrichEnabled) //
                .userId(request.getUserId()) //
                .build();
        return workflowJobService.submit(configuration, pidWrapper.getPid());
    }

}
