package com.latticeengines.apps.cdl.workflow;


import static com.latticeengines.domain.exposed.metadata.DataCollectionArtifact.FULL_PROFILE;
import static com.latticeengines.domain.exposed.metadata.DataCollectionArtifact.Status.GENERATING;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.DataCollectionService;
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

@Component
public class AtlasProfileReportWorkflowSubmitter extends WorkflowSubmitter {

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private ZKConfigService zkConfigService;

    @WithWorkflowJobPid
    public ApplicationId submit(@NotNull String customerSpace,
                                             @NotNull AtlasProfileReportRequest request,
                                             @NotNull WorkflowPidWrapper pidWrapper) {
        DataCollection.Version activeVersion = dataCollectionService.getActiveVersion(customerSpace);
        DataCollectionArtifact artifact = //
                dataCollectionService.getLatestArtifact(customerSpace, FULL_PROFILE, activeVersion);
        if (artifact != null && !artifact.getStatus().isTerminal()) {
            throw new IllegalStateException("There is already a job running to generate profile report.");
        }
        boolean internalEnrichEnabled = zkConfigService.isInternalEnrichmentEnabled(CustomerSpace.parse(customerSpace));
        AtlasProfileReportWorkflowConfiguration configuration = new AtlasProfileReportWorkflowConfiguration.Builder() //
                .customer(CustomerSpace.parse(customerSpace)) //
                .allowInternalEnrichAttrs(internalEnrichEnabled) //
                .userId(request.getUserId()) //
                .build();
        ApplicationId applicationId = workflowJobService.submit(configuration, pidWrapper.getPid());

        artifact = new DataCollectionArtifact();
        artifact.setName(FULL_PROFILE);
        artifact.setUrl(null);
        artifact.setStatus(GENERATING);
        dataCollectionService.createArtifact(customerSpace, artifact.getName(), artifact.getUrl(),
                artifact.getStatus(), activeVersion);

        return applicationId;
    }

}
