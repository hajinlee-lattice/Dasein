package com.latticeengines.apps.dcp.workflow;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.apps.dcp.service.SourceService;
import com.latticeengines.apps.dcp.service.UploadService;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.DCPImportRequest;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.domain.exposed.serviceflows.dcp.DCPSourceImportWorkflowConfiguration;

@Component
public class DCPSourceImportWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(DCPSourceImportWorkflowSubmitter.class);

    private static final String DEFAULT_DCP_S3_USER = "Default_DCP_S3_User";

    // TODO: Need Action?
//    @Inject
//    private TenantService tenantService;
//
//    @Inject
//    private ActionService actionService;

    @Inject
    private SourceService sourceService;

    @Inject
    private UploadService uploadService;

    @WithWorkflowJobPid
    public ApplicationId submit(CustomerSpace customerSpace, DCPImportRequest importRequest,
                                WorkflowPidWrapper pidWrapper) {
        UploadConfig uploadConfig = new UploadConfig();
        uploadConfig.setDropFilePath(importRequest.getS3FileKey());
        Upload upload = uploadService.createUpload(customerSpace.toString(), importRequest.getSourceId(), uploadConfig);
        DCPSourceImportWorkflowConfiguration configuration =
                generateConfiguration(customerSpace, importRequest.getProjectId(), importRequest.getSourceId(), upload.getPid());

        ApplicationId applicationId = workflowJobService.submit(configuration, pidWrapper.getPid());
        UploadStats uploadStats = new UploadStats();
        uploadStats.setImportApplicationPid(pidWrapper.getPid());
        uploadService.updateUploadStats(customerSpace.toString(), upload.getPid(), uploadStats);
        return applicationId;
    }

    private DCPSourceImportWorkflowConfiguration generateConfiguration(CustomerSpace customerSpace, String projectId,
                                                                       String sourceId, Long uploadPid) {
        Preconditions.checkNotNull(uploadPid);
        Preconditions.checkArgument(StringUtils.isNotBlank(projectId));
        Preconditions.checkArgument(StringUtils.isNotBlank(sourceId));
        return new DCPSourceImportWorkflowConfiguration.Builder()
                .customer(customerSpace) //
                .internalResourceHostPort(internalResourceHostPort) //
                .microServiceHostPort(microserviceHostPort) //
                .userId(DEFAULT_DCP_S3_USER) //
                .projectId(projectId) //
                .sourceId(sourceId) //
                .uploadPid(uploadPid) //
                .build();
    }


}
