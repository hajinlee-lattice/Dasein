package com.latticeengines.apps.cdl.workflow;

import java.util.Arrays;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.EntityExportRequest;
import com.latticeengines.domain.exposed.cdl.ExportEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.EntityExportWorkflowConfiguration;

@Component
public class EntityExportWorkflowSubmitter extends WorkflowSubmitter {

    @WithWorkflowJobPid
    public ApplicationId submit(@NotNull String customerSpace, @NotNull EntityExportRequest request,
                                @NotNull WorkflowPidWrapper pidWrapper) {
        EntityExportWorkflowConfiguration configuration = configure(customerSpace, request);
        return workflowJobService.submit(configuration, pidWrapper.getPid());
    }

    @VisibleForTesting
    private EntityExportWorkflowConfiguration configure(String customerSpace, EntityExportRequest request) {
        return new EntityExportWorkflowConfiguration.Builder() //
                .customer(CustomerSpace.parse(customerSpace)) //
                .exportEntities(Arrays.asList(ExportEntity.Account, ExportEntity.Contact)) //
                .dataCollectionVersion(request.getDataCollectionVersion()) //
                .frontEndQuery(null) //
                .compressResult(true) //
                .saveToDropfolder(true) //
                .build();
    }

}
