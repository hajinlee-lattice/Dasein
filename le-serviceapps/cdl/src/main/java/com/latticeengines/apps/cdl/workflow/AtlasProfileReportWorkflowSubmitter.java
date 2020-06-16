package com.latticeengines.apps.cdl.workflow;


import static com.latticeengines.domain.exposed.metadata.DataCollectionArtifact.FULL_PROFILE;

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
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;
import com.latticeengines.domain.exposed.serviceflows.cdl.AtlasProfileReportWorkflowConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

@Component
public class AtlasProfileReportWorkflowSubmitter extends WorkflowSubmitter {

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private ZKConfigService zkConfigService;

    @WithWorkflowJobPid
    public ApplicationId submit(@NotNull String customerSpace,
                                             @NotNull AtlasProfileReportRequest request,
                                             @NotNull WorkflowPidWrapper pidWrapper) {
        DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(customerSpace);
        DataCollectionArtifact artifact = dataCollectionProxy.getDataCollectionArtifact(
                customerSpace, FULL_PROFILE, activeVersion);
        if (artifact != null && !artifact.getStatus().isTerminal()) {
            throw new IllegalStateException("There is already a job running to generate profile report.");
        }
        boolean internalEnrichEnabled = zkConfigService.isInternalEnrichmentEnabled(CustomerSpace.parse(customerSpace));
        AtlasProfileReportWorkflowConfiguration configuration = new AtlasProfileReportWorkflowConfiguration.Builder() //
                .customer(CustomerSpace.parse(customerSpace)) //
                .allowInternalEnrichAttrs(internalEnrichEnabled) //
                .userId(request.getUserId()) //
                .build();
        return workflowJobService.submit(configuration, pidWrapper.getPid());
    }

}
