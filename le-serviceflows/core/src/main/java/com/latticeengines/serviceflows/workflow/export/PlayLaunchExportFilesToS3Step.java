package com.latticeengines.serviceflows.workflow.export;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.latticeengines.aws.sns.SNSService;
import com.latticeengines.domain.exposed.cdl.ExternalIntegrationMessageAttribute;
import com.latticeengines.domain.exposed.cdl.ExternalIntegrationWorkflowType;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.serviceflows.cdl.PlayLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchExportFilesToS3Configuration;

@Component("playLaunchExportFilesToS3Step")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PlayLaunchExportFilesToS3Step extends BaseImportExportS3<PlayLaunchExportFilesToS3Configuration> {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchExportFilesToS3Step.class);

    private List<String> s3ExportFilePaths = new ArrayList<>();

    private String STRING = "String";

    @Inject
    private SNSService snsService;

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

    private void publishToSnsTopic() {
        PlayLaunchExportFilesToS3Configuration config = getConfiguration();
        String playLaunchId = config.getPlayLaunchId();
        String externalSystemId = config.getDestinationOrgId();
        s3ExportFilePaths.stream().forEach(exportPath -> {
            Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
            // TODO: Replace with destination org type
            messageAttributes.put(ExternalIntegrationMessageAttribute.TARGET_SYSTEMS.getName(),
                    new MessageAttributeValue().withDataType(STRING).withStringValue("Marketo"));
            messageAttributes.put(ExternalIntegrationMessageAttribute.OPERATION.getName(),
                    new MessageAttributeValue().withDataType(STRING)
                            .withStringValue(ExternalIntegrationWorkflowType.EXPORT.toString()));
            messageAttributes.put(ExternalIntegrationMessageAttribute.TENANT_ID.getName(),
                    new MessageAttributeValue().withDataType(STRING).withStringValue(tenantId));
            messageAttributes.put(ExternalIntegrationMessageAttribute.ENTITY_NAME.getName(),
                    new MessageAttributeValue().withDataType(STRING).withStringValue(PlayLaunch.class.getSimpleName()));
            messageAttributes.put(ExternalIntegrationMessageAttribute.ENTITY_ID.getName(),
                    new MessageAttributeValue().withDataType(STRING).withStringValue(playLaunchId));
            messageAttributes.put(ExternalIntegrationMessageAttribute.EXTERNAL_SYSTEM_ID.getName(),
                    new MessageAttributeValue().withDataType(STRING).withStringValue(externalSystemId));
            messageAttributes.put(ExternalIntegrationMessageAttribute.SOURCE_FILE.getName(),
                    new MessageAttributeValue().withDataType(STRING).withStringValue(exportPath));

            try {
                log.info(String.format("Publishing play launch id %s to destination org id %s ", playLaunchId,
                        externalSystemId));
                snsService.publishToTopic("ExportDataTopic", exportPath.substring(exportPath.indexOf("dropfolder")),
                        messageAttributes);
            } catch (Exception e) {
                log.info(e.getMessage());
            }
        });
    }


    @Override
    public void execute() {
        super.execute();
        publishToSnsTopic();
    }

}
