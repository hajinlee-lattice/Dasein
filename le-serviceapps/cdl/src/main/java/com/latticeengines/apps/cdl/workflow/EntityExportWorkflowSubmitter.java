package com.latticeengines.apps.cdl.workflow;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import com.latticeengines.domain.exposed.serviceflows.cdl.EntityExportWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.SegmentExportWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;

@Component
public class EntityExportWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(EntityExportWorkflowSubmitter.class);

    @Inject
    private AtlasExportService atlasExportService;

    @WithWorkflowJobPid
    public ApplicationId submit(@NotNull String customerSpace, @NotNull EntityExportRequest request,
                                @NotNull WorkflowPidWrapper pidWrapper) {
        AtlasExport atlasExport;
        if (StringUtils.isEmpty(request.getAtlasExportId())) {
            throw new NullPointerException("export id should not be null");
        } else {
            atlasExport = atlasExportService.getAtlasExport(customerSpace, request.getAtlasExportId());
        }
        EntityExportWorkflowConfiguration configuration = configure(customerSpace, request, atlasExport, request.isSaveToDropfolder());

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "entityExportWorkflow");
        inputProperties.put(SegmentExportWorkflowConfiguration.SEGMENT_EXPORT_ID, //
                atlasExport.getUuid());
        inputProperties.put(SegmentExportWorkflowConfiguration.EXPORT_OBJECT_TYPE, //
                atlasExport.getExportType().getDisplayName());
        inputProperties.put(SegmentExportWorkflowConfiguration.EXPIRE_BY_UTC_TIMESTAMP, //
                Long.toString(atlasExport.getCleanupBy().getTime()));
        inputProperties.put(SegmentExportWorkflowConfiguration.SEGMENT_DISPLAY_NAME, //
                atlasExport.getSegmentName());
        configuration.setInputProperties(inputProperties);
        ApplicationId applicationId = workflowJobService.submit(configuration, pidWrapper.getPid());
        atlasExport.setApplicationId(applicationId.toString());
        log.info(String.format("The current status for atlas export is %s", atlasExport.getStatus()));
        atlasExportService.updateAtlasExport(customerSpace, atlasExport);
        return applicationId;
    }

    @VisibleForTesting
    private EntityExportWorkflowConfiguration configure(String customerSpace, EntityExportRequest request,
                                                        AtlasExport atlasExport, Boolean saveToDropfolder) {
        return new EntityExportWorkflowConfiguration.Builder()
                .customer(CustomerSpace.parse(customerSpace))
                .userId(atlasExport.getCreatedBy())
                .dataCollectionVersion(request.getDataCollectionVersion())
                .compressResult(true)
                .saveToDropfolder(saveToDropfolder)
                .atlasExportId(atlasExport.getUuid())
                .build();
    }

}
