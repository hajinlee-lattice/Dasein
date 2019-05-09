package com.latticeengines.serviceflows.workflow.export;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.velocity.shaded.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.ExternalIntegrationWorkflowType;
import com.latticeengines.domain.exposed.cdl.MessageType;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.PlayLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchExportFilesToS3Configuration;
import com.latticeengines.proxy.exposed.cdl.DataIntegrationMonitoringProxy;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("playLaunchExportFilesToS3Step")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PlayLaunchExportFilesToS3Step extends BaseImportExportS3<PlayLaunchExportFilesToS3Configuration> {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchExportFilesToS3Step.class);

    private List<String> s3ExportFilePaths = new ArrayList<>();

    private String CSV = "csv";

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DataIntegrationMonitoringProxy dataIntegrationMonitoringProxy;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        List<String> exportFiles = getListObjectFromContext(PlayLaunchWorkflowConfiguration.RECOMMENDATION_EXPORT_FILES,
                String.class);
        if (exportFiles == null || exportFiles.isEmpty()) {
            return;
        }
        log.info("Uploading all HDFS files to S3. {}", exportFiles);
        exportFiles.forEach(hdfsFilePath -> {
            ImportExportRequest request = new ImportExportRequest();
            request.srcPath = hdfsFilePath;
            request.tgtPath = pathBuilder.convertAtlasFileExport(hdfsFilePath, podId, tenantId, dropBoxSummary,
                    exportS3Bucket);
            requests.add(request);
            // Collect all S3 FilePaths
            s3ExportFilePaths.add(request.tgtPath);
        });
        log.info("Uploaded S3 Files. {}", s3ExportFilePaths);

    }

    public void registerAndPublishExportRequest() {
        PlayLaunchExportFilesToS3Configuration config = getConfiguration();
        CustomerSpace customerSpace = config.getCustomerSpace();
        String playLaunchId = config.getPlayLaunchId();
        LookupIdMap lookupIdMap = config.getLookupIdMap();
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace.toString());

        DataIntegrationStatusMonitorMessage message = new DataIntegrationStatusMonitorMessage();
        String workflowRequestId = UUID.randomUUID().toString();
        message.setWorkflowRequestId(workflowRequestId);
        message.setTenantName(tenant.getName());
        message.setOperation(ExternalIntegrationWorkflowType.EXPORT.toString());
        message.setEntityId(playLaunchId);
        message.setEntityName(PlayLaunch.class.getSimpleName());
        message.setExternalSystemId(lookupIdMap.getOrgId());
        String sourceFile = s3ExportFilePaths.stream().filter(path -> FilenameUtils.getExtension(path).equals(CSV))
                .findFirst().get();
        message.setSourceFile(sourceFile.substring(sourceFile.indexOf("dropfolder")));
        message.setEventType(DataIntegrationEventType.WorkflowSubmitted.toString());
        message.setEventTime(new Date());
        message.setMessageType(MessageType.Event.toString());
        message.setMessage(String.format("Workflow Request Id has been launched to %s", workflowRequestId,
                lookupIdMap.getOrgId()));
        message.setEventDetail(null);
        dataIntegrationMonitoringProxy.createOrUpdateStatus(message);
        putStringValueInContext(PlayLaunchWorkflowConfiguration.RECOMMENDATION_WORKFLOW_REQUEST_ID, workflowRequestId);
        putObjectInContext(PlayLaunchWorkflowConfiguration.RECOMMENDATION_S3_EXPORT_FILE_PATHS, s3ExportFilePaths);
        log.info(JsonUtils.serialize(message));
    }

    @Override
    public void execute() {
        super.execute();
        registerAndPublishExportRequest();
    }

    @VisibleForTesting
    public void setS3ExportFiles(List<String> exportFiles) {
        s3ExportFilePaths = exportFiles;
    }

}
