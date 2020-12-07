package com.latticeengines.apps.cdl.workflow;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.PublishVIDataWorkflowConfiguration;

@Component
public class PublishVIDataWorkflowSubmitter extends WorkflowSubmitter {

    @Inject
    private DataCollectionService dataCollectionService;

    @WithWorkflowJobPid
    public ApplicationId submitPublishViData(@NotNull String customerSpace, DataCollection.Version version,
                                             @NotNull WorkflowPidWrapper pidWrapper) {
        if (version == null) {
            version = dataCollectionService.getActiveVersion(customerSpace);
        }
        PublishVIDataWorkflowConfiguration configuration = new PublishVIDataWorkflowConfiguration.Builder() //
                .customer(CustomerSpace.parse(customerSpace)) //
                .version(version) //
                .build();
        return workflowJobService.submit(configuration, pidWrapper.getPid());
    }
}
