package com.latticeengines.apps.cdl.workflow;

import java.util.Arrays;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.cdl.service.AtlasExportService;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.cdl.EntityExportRequest;
import com.latticeengines.domain.exposed.cdl.ExportEntity;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.serviceflows.cdl.EntityExportWorkflowConfiguration;

@Component
public class EntityExportWorkflowSubmitter extends WorkflowSubmitter {

    @Inject
    private AtlasExportService atlasExportService;

    @WithWorkflowJobPid
    public ApplicationId submit(@NotNull String customerSpace, @NotNull EntityExportRequest request,
                                @NotNull WorkflowPidWrapper pidWrapper) {
        AtlasExport atlasExport;
        if (StringUtils.isEmpty(request.getAtlasExportId())) {
            atlasExport = atlasExportService.createAtlasExport(customerSpace,
                    AtlasExportType.ACCOUNT_AND_CONTACT);
        } else {
            atlasExport = atlasExportService.getAtlasExport(customerSpace, request.getAtlasExportId());
        }
        EntityExportWorkflowConfiguration configuration = configure(customerSpace, request, atlasExport);
        ApplicationId applicationId = workflowJobService.submit(configuration, pidWrapper.getPid());
        atlasExport.setApplicationId(applicationId.toString());
        atlasExportService.updateAtlasExport(customerSpace, atlasExport);
        return applicationId;
    }

    @VisibleForTesting
    private EntityExportWorkflowConfiguration configure(String customerSpace, EntityExportRequest request,
                                                        AtlasExport atlasExport) {
        return new EntityExportWorkflowConfiguration.Builder() //
                .customer(CustomerSpace.parse(customerSpace)) //
                .exportEntities(Arrays.asList(ExportEntity.Account, ExportEntity.Contact)) //
                .dataCollectionVersion(request.getDataCollectionVersion()) //
                .compressResult(true) //
                .saveToDropfolder(true) //
                .atlasExportId(atlasExport.getUuid())
                .build();
    }

}
