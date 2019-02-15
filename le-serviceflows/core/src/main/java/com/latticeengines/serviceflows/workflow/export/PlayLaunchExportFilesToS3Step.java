package com.latticeengines.serviceflows.workflow.export;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.latticeengines.aws.sns.SNSService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.ExternalIntegrationMessageAttribute;
import com.latticeengines.domain.exposed.cdl.ExternalIntegrationWorkflowType;
import com.latticeengines.domain.exposed.cdl.MessageType;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.PlayLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchExportFilesToS3Configuration;
import com.latticeengines.proxy.exposed.cdl.DataIntegrationMonitoringProxy;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.cdl.ExternalSystemAuthenticationProxy;

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
    private ExternalSystemAuthenticationProxy externalSystemAuthenticationProxy;

    @Inject
    private DropBoxProxy dropBoxProxy;

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

    private void createStatusMonitor() {
        PlayLaunchExportFilesToS3Configuration config = getConfiguration();
        CustomerSpace customerSpace = config.getCustomerSpace();
        String playLaunchId = config.getPlayLaunchId();
        String externalSystemId = config.getLookupIdMap().getOrgId();
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace.toString());

        s3ExportFilePaths.stream().forEach(exportPath -> {
            DataIntegrationStatusMonitorMessage message = new DataIntegrationStatusMonitorMessage();
            String workflowRequestId = UUID.randomUUID().toString();
            message.setWorkflowRequestId(workflowRequestId);
            message.setTenantId(tenant.getName());
            message.setOperation(ExternalIntegrationWorkflowType.EXPORT.toString());
            message.setEntityId(playLaunchId);
            message.setEntityName(PlayLaunch.class.getSimpleName());
            message.setExternalSystemId(externalSystemId);
            message.setSourceFile(exportPath.substring(exportPath.indexOf("dropfolder")));
            message.setEventType(DataIntegrationEventType.WORKFLOW_SUBMITTED.toString());
            message.setEventTime(new Date());
            message.setMessageType(MessageType.EVENT.toString());
            message.setMessage(
                    String.format("Workflow Request Id has been launched to %s", workflowRequestId, externalSystemId));
            dataIntegrationMonitoringProxy.createOrUpdateStatus(message);
            log.info(JsonUtils.serialize(message));
            publishToSnsTopic(customerSpace.toString(), workflowRequestId, exportPath);
        });
    }

    private void publishToSnsTopic(String customerSpace, String workflowRequestId, String exportPath) {
        Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
        // TODO: Replace with destination org type
        String dropfolder = exportPath.substring(exportPath.indexOf("dropfolder"));
        messageAttributes.put(ExternalIntegrationMessageAttribute.WORKFLOW_REQ_ID.getName(),
                new MessageAttributeValue().withDataType(STRING).withStringValue(workflowRequestId));
        messageAttributes.put(ExternalIntegrationMessageAttribute.TARGET_SYSTEMS.getName(),
                new MessageAttributeValue().withDataType(STRING).withStringValue("Marketo"));
        messageAttributes.put(ExternalIntegrationMessageAttribute.OPERATION.getName(), new MessageAttributeValue()
                .withDataType(STRING).withStringValue(ExternalIntegrationWorkflowType.EXPORT.toString()));
        messageAttributes.put(ExternalIntegrationMessageAttribute.SOURCE_FILE.getName(), new MessageAttributeValue()
                .withDataType(STRING).withStringValue(dropfolder));

        try {
            log.info(String.format("Publishing play launch with workflow request id %s ", workflowRequestId));
            snsService.publishToTopic("ExportDataTopic", dropfolder, messageAttributes);
        } catch (Exception e) {
            log.info(e.getMessage());
        }
    }


    @Override
    public void execute() {
        super.execute();
        createStatusMonitor();
    }

}
