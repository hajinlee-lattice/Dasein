package com.latticeengines.serviceflows.workflow.export;

import java.util.ArrayList;
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

import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.aws.sns.SNSService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.ExportFileConfig;
import com.latticeengines.domain.exposed.cdl.ExternalIntegrationMessageAttribute;
import com.latticeengines.domain.exposed.cdl.ExternalIntegrationMessageBody;
import com.latticeengines.domain.exposed.cdl.ExternalIntegrationWorkflowType;
import com.latticeengines.domain.exposed.cdl.MessageType;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.PlayLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchExportFilesToS3Configuration;
import com.latticeengines.proxy.exposed.cdl.DataIntegrationMonitoringProxy;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;

@Component("playLaunchExportFilesToS3Step")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PlayLaunchExportFilesToS3Step extends BaseImportExportS3<PlayLaunchExportFilesToS3Configuration> {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchExportFilesToS3Step.class);

    private List<String> s3ExportFilePaths = new ArrayList<>();

    private String STRING = "String";

    @Inject
    private SNSService snsService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private DataIntegrationMonitoringProxy dataIntegrationMonitoringProxy;

    @Value("${aws.data.integration.exportdata.topic}")
    protected String exportDataTopic;

    private Map<String, List<ExportFileConfig>> sourceFiles = new HashMap<String, List<ExportFileConfig>>();

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        List<String> exportFiles = getListObjectFromContext(PlayLaunchWorkflowConfiguration.RECOMMENDATION_EXPORT_FILES,
                String.class);
        if (exportFiles == null || exportFiles.isEmpty()) {
            return;
        }
        log.info("Uploading all HDFS files to S3. {}", exportFiles);
        exportFiles.stream().forEach(hdfsFilePath -> {
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
        message.setTenantId(tenant.getName());
        message.setOperation(ExternalIntegrationWorkflowType.EXPORT.toString());
        message.setEntityId(playLaunchId);
        message.setEntityName(PlayLaunch.class.getSimpleName());
        message.setExternalSystemId(lookupIdMap.getOrgId());
        message.setSourceFile(s3ExportFilePaths.toString());
        message.setEventType(DataIntegrationEventType.WORKFLOW_SUBMITTED.toString());
        message.setEventTime(new Date());
        message.setMessageType(MessageType.EVENT.toString());
        message.setMessage(String.format("Workflow Request Id has been launched to %s", workflowRequestId,
                lookupIdMap.getOrgId()));
        dataIntegrationMonitoringProxy.createOrUpdateStatus(message);
        log.info(JsonUtils.serialize(message));
        publishToSnsTopic(customerSpace.toString(), workflowRequestId);
    }

    public PublishResult publishToSnsTopic(String customerSpace, String workflowRequestId) {
        PlayLaunchExportFilesToS3Configuration config = getConfiguration();
        LookupIdMap lookupIdMap = config.getLookupIdMap();

        Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
        messageAttributes.put(ExternalIntegrationMessageAttribute.TARGET_SYSTEMS.getName(),
                new MessageAttributeValue().withDataType(STRING)
                        .withStringValue(config.getLookupIdMap().getExternalSystemName().name()));

        s3ExportFilePaths.stream().forEach(exportPath -> {
            List<ExportFileConfig> fileConfigs = sourceFiles.getOrDefault(FilenameUtils.getExtension(exportPath),
                    new ArrayList<ExportFileConfig>());
            fileConfigs.add(new ExportFileConfig(exportPath.substring(exportPath.indexOf("dropfolder")),
                    exportS3Bucket));
            sourceFiles.put(FilenameUtils.getExtension(exportPath), fileConfigs);
        });


        DropBoxSummary dropboxSummary = dropBoxProxy.getDropBox(customerSpace);
        ExternalIntegrationMessageBody messageBody = new ExternalIntegrationMessageBody();
        messageBody.setWorkflowRequestId(workflowRequestId);
        messageBody.setSourceFiles(sourceFiles);
        messageBody.setTrayTenantId(dropboxSummary.getDropBox());
        if (lookupIdMap != null && lookupIdMap.getExternalAuthentication() != null) {
            messageBody.setSolutionInstanceId(lookupIdMap.getExternalAuthentication().getSolutionInstanceId());
        }
        messageBody.setExternalAudienceId(config.getExternalAudienceId());
        messageBody.setExternalAudienceName(config.getExternalAudienceName());

        Map<String, Object> jsonMessage = new HashMap<>();
        jsonMessage.put("default", JsonUtils.serialize(messageBody));

        try {
            PublishRequest publishRequest = new PublishRequest().withMessage(JsonUtils.serialize(jsonMessage))
                    .withMessageStructure("json").withMessageAttributes(messageAttributes)
                    .withMessageAttributes(messageAttributes);
            log.info(String.format("Publishing play launch with workflow request id %s to Topic: %s", workflowRequestId, exportDataTopic));
            log.info("Publish Request: " + JsonUtils.serialize(publishRequest));
            return snsService.publishToTopic(exportDataTopic, publishRequest);
        } catch (Exception e) {
            log.info(e.toString());
            return null;
        }
    }


    @Override
    public void execute() {
        super.execute();
        registerAndPublishExportRequest();
    }

    @VisibleForTesting
    public void setDropBoxProxy(DropBoxProxy dropBoxProxy) {
        this.dropBoxProxy = dropBoxProxy;
    }

    @VisibleForTesting
    public void setS3ExportFiles(List<String> exportFiles) {
        s3ExportFilePaths = exportFiles;
    }

}
