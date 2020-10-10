package com.latticeengines.cdl.workflow.steps.play;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.velocity.shaded.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.aws.s3.S3Service;
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
import com.latticeengines.domain.exposed.serviceflows.cdl.CampaignLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.DeltaCampaignLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.PlayLaunchExportFilesToS3Configuration;
import com.latticeengines.proxy.exposed.cdl.DataIntegrationMonitoringProxy;
import com.latticeengines.serviceflows.workflow.export.BaseImportExportS3;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("playLaunchExportFilesToS3Step")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CampaignLaunchExportFilesToS3Step extends BaseImportExportS3<PlayLaunchExportFilesToS3Configuration> {

    private static final Logger log = LoggerFactory.getLogger(CampaignLaunchExportFilesToS3Step.class);

    private List<String> s3ExportFilePaths = new ArrayList<>();

    private String CSV = "csv";

    private String FOLDER = "dropfolder";

    private static final String CDL_DATA_INTEGRATION_END_POINT = "/cdl/dataintegration";

    @Value("${cdl.atlas.export.dropfolder.tag}")
    private String expire30dTag;

    @Value("${cdl.atlas.export.dropfolder.tag.value}")
    private String expire30dTagValue;

    @Value("${common.microservice.url}")
    private String microserviceHostPort;

    @Value("${cdl.campaign.integration.session.context.dynamo.table}")
    private String integrationSessionContextTable;

    @Value("${cdl.campaign.integration.session.context.ttl}")
    private long sessionContextTTLinSec;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DataIntegrationMonitoringProxy dataIntegrationMonitoringProxy;

    @Inject
    private S3Service s3Service;

    @Inject
    private DynamoItemService dynamoItemService;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        List<String> exportFiles = getListObjectFromContext(CampaignLaunchWorkflowConfiguration.RECOMMENDATION_EXPORT_FILES,
                String.class);
        if (exportFiles == null || exportFiles.isEmpty()) {
            return;
        }

        log.info("Uploading all HDFS files to S3. {}", exportFiles);
        LookupIdMap lookupIdMap = getConfiguration().getLookupIdMap();
        if (lookupIdMap.isTrayEnabled()) {
            exportFiles.forEach(hdfsFilePath -> {
                ImportExportRequest request = new ImportExportRequest();
                request.srcPath = hdfsFilePath;
                request.tgtPath = pathBuilder.convertAtlasFileExport(hdfsFilePath, podId, tenantId, dropBoxSummary,
                        exportS3Bucket);
                requests.add(request);
                // Collect all S3 FilePaths
                s3ExportFilePaths.add(request.tgtPath);
            });
        } else {
            exportFiles.forEach(hdfsFilePath -> {
                ImportExportRequest request = new ImportExportRequest();
                request.srcPath = hdfsFilePath;
                request.tgtPath = pathBuilder.convertS3CampaignExportDir(hdfsFilePath, s3Bucket,
                        dropBoxSummary.getDropBox(), getConfiguration().getPlayName(),
                        getConfiguration().getPlayDisplayName());
                requests.add(request);
                // Collect all S3 FilePaths
                s3ExportFilePaths.add(request.tgtPath);
            });
        }

        log.info("Uploaded S3 Files. {}", s3ExportFilePaths);

    }

    private void registerAndPublishExportRequest() {
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
        message.setSourceFile(sourceFile.substring(sourceFile.indexOf(FOLDER)));
        message.setEventType(DataIntegrationEventType.WorkflowSubmitted.toString());
        message.setEventTime(new Date());
        message.setMessageType(MessageType.Event.toString());
        message.setMessage(String.format("Workflow Request Id %s has been launched to %s", workflowRequestId,
                lookupIdMap.getOrgId()));
        message.setEventDetail(null);
        List<DataIntegrationStatusMonitorMessage> messages = Collections.singletonList(message);
        log.info(String.format("Creating status monitor for launchId %s with workflowRequestId %s", playLaunchId,
                workflowRequestId));
        dataIntegrationMonitoringProxy.createOrUpdateStatus(messages);
        putStringValueInContext(CampaignLaunchWorkflowConfiguration.RECOMMENDATION_WORKFLOW_REQUEST_ID, workflowRequestId);
        putObjectInContext(CampaignLaunchWorkflowConfiguration.RECOMMENDATION_S3_EXPORT_FILE_PATHS, s3ExportFilePaths);
        log.info(JsonUtils.serialize(message));
    }

    @Override
    public void execute() {
        super.execute();
        tagCreatedS3Objects();
        registerAndPublishExportRequest();
        publishSessionContext();
    }

    private void publishSessionContext() {
        String workflowRequestId = getStringValueFromContext(
                DeltaCampaignLaunchWorkflowConfiguration.RECOMMENDATION_WORKFLOW_REQUEST_ID);
        log.info(String.format("Publish to DynamoDB %s with workflowRequestId %s and Url %s",
                integrationSessionContextTable, workflowRequestId,
                microserviceHostPort + CDL_DATA_INTEGRATION_END_POINT));
        dynamoItemService.putItem(integrationSessionContextTable, getItem(workflowRequestId));
    }

    private Item getItem(String workflowRequestId) {
        Map<String, Object> session = new HashMap<String, Object>();
        session.put("Url", microserviceHostPort + CDL_DATA_INTEGRATION_END_POINT);
        session.put("Mapping", "");
        return new Item().withPrimaryKey("WorkflowId", workflowRequestId)
                .withLong("TTL", System.currentTimeMillis() / 1000 + sessionContextTTLinSec)
                .withString("Session", JsonUtils.serialize(session));
    }

    private void tagCreatedS3Objects() {
        log.info("Tagging the created s3 files to expire in 30 days");
        s3ExportFilePaths.forEach(s3Path -> {
            try {
                s3Service.addTagToObject(s3Bucket, extractBucketLessPath(s3Path), expire30dTag, expire30dTagValue);
                log.info(String.format("Tagged %s to expire in 30 days", extractBucketLessPath(s3Path)));
            } catch (Exception e) {
                log.error(String.format("Failed to tag %s to expire in 30 days", s3Path));
            }
        });
    }

    private String extractBucketLessPath(String s3Path) {
        return s3Path.replace(pathBuilder.getProtocol() + pathBuilder.getProtocolSeparator()
                + pathBuilder.getPathSeparator() + s3Bucket + pathBuilder.getPathSeparator(), "");
    }

    @VisibleForTesting
    public void setS3ExportFiles(List<String> exportFiles) {
        s3ExportFilePaths = exportFiles;
    }

}
