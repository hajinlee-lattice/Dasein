package com.latticeengines.cdl.workflow.steps.play;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.aws.sns.SNSService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.cdl.operationflow.service.impl.ChannelConfigProcessor;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.ExportFileConfig;
import com.latticeengines.domain.exposed.cdl.ExternalIntegrationMessageBody;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.serviceflows.cdl.DeltaCampaignLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.DeltaCampaignLaunchExportPublishToSNSConfiguration;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;
import com.latticeengines.workflow.exposed.util.WorkflowJobUtils;

@Component("deltaCampaignLaunchExportPublishToSNSStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DeltaCampaignLaunchExportPublishToSNSStep
        extends BaseWorkflowStep<DeltaCampaignLaunchExportPublishToSNSConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(DeltaCampaignLaunchExportPublishToSNSStep.class);

    @Inject
    private BatonService batonService;
    @Inject
    private SNSService snsService;
    @Inject
    private ChannelConfigProcessor channelConfigProcessor;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Value("${aws.data.integration.exportdata.topic}")
    protected String exportDataTopic;

    @Value("${aws.customer.export.s3.bucket}")
    protected String exportS3Bucket;

    private boolean createAddCsvDataFrame;

    private boolean createDeleteCsvDataFrame;

    private boolean createTaskDescriptionFile;

    @Override
    public void execute() {
        DeltaCampaignLaunchExportPublishToSNSConfiguration config = getConfiguration();
        CustomerSpace customerSpace = config.getCustomerSpace();
        String workflowRequestId = getStringValueFromContext(
                DeltaCampaignLaunchWorkflowConfiguration.RECOMMENDATION_WORKFLOW_REQUEST_ID);

        publishToSnsTopic(customerSpace, workflowRequestId);
    }

    public PublishResult publishToSnsTopic(CustomerSpace customerSpace, String workflowRequestId) {
        createAddCsvDataFrame = Boolean.toString(true)
                .equals(getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_ADD_CSV_DATA_FRAME));
        createDeleteCsvDataFrame = Boolean.toString(true).equals(
                getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_DELETE_CSV_DATA_FRAME));
        createTaskDescriptionFile = Boolean.toString(true)
                .equals(getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_TASK_DESCRIPTION_FILE));
        log.info("createAddCsvDataFrame=" + createAddCsvDataFrame + ", createDeleteCsvDataFrame="
                + createDeleteCsvDataFrame + ", createTaskDescriptionFile=" + createTaskDescriptionFile);

        DeltaCampaignLaunchExportPublishToSNSConfiguration config = getConfiguration();
        LookupIdMap lookupIdMap = config.getLookupIdMap();

        DropBoxSummary dropboxSummary = dropBoxProxy.getDropBox(customerSpace.toString());
        ExternalIntegrationMessageBody messageBody = new ExternalIntegrationMessageBody();
        if (createAddCsvDataFrame) {
            Map<String, List<ExportFileConfig>> sourceFiles = getFiles(DeltaCampaignLaunchWorkflowConfiguration.ADD);
            messageBody.setSourceFiles(sourceFiles);
        }
        if (createDeleteCsvDataFrame) {
            Map<String, List<ExportFileConfig>> deleteFiles = getFiles(DeltaCampaignLaunchWorkflowConfiguration.DELETE);
            messageBody.setDeleteFiles(deleteFiles);
        }
        if (createTaskDescriptionFile) {
            Map<String, List<ExportFileConfig>> taskDescription = getFiles(DeltaCampaignLaunchWorkflowConfiguration.TASK_DESCRIPTION);
            messageBody.setTaskDescription(taskDescription);
        }

        messageBody.setWorkflowRequestId(workflowRequestId);
        messageBody.setTrayTenantId(dropboxSummary.getDropBox());
        if (lookupIdMap != null && lookupIdMap.getExternalAuthentication() != null) {
            messageBody.setSolutionInstanceId(lookupIdMap.getExternalAuthentication().getSolutionInstanceId());
        }
        messageBody.setFolderId(config.getExternalFolderId());
        messageBody.setFolderName(config.getExternalFolderName());
        messageBody.setExternalAudienceId(config.getExternalAudienceId());
        messageBody.setExternalAudienceName(config.getExternalAudienceName());
        messageBody.setEnableAcxiom(checkAcxiomFeatureFlag(lookupIdMap.getExternalSystemName(), customerSpace));
        channelConfigProcessor.updateSnsMessageWithChannelConfig(config.getChannelConfig(), messageBody);
        setOutreachTaskSettings(messageBody, customerSpace);

        Map<String, Object> jsonMessage = new HashMap<>();
        jsonMessage.put("default", JsonUtils.serialize(messageBody));

        try {
            PublishRequest publishRequest = new PublishRequest().withMessage(JsonUtils.serialize(jsonMessage))
                    .withMessageStructure("json");
            log.info(String.format("Publishing play launch with workflow request id %s to Topic: %s", workflowRequestId,
                    exportDataTopic));
            log.info("Publish Request: " + JsonUtils.serialize(publishRequest));
            return snsService.publishToTopic(exportDataTopic, publishRequest);
        } catch (Exception e) {
            log.info(e.toString());
            return null;
        }
    }

    @SuppressWarnings("rawtypes")
    private Map<String, List<ExportFileConfig>> getFiles(String key) {
        Map<String, List<ExportFileConfig>> retFiles = new HashMap<>();
        Map<String, List> exportFiles = getMapObjectFromContext(
                DeltaCampaignLaunchWorkflowConfiguration.ADD_AND_DELETE_S3_EXPORT_FILES, String.class, List.class);
        log.info("exportFiles=" + exportFiles);
        if (MapUtils.isNotEmpty(exportFiles) && CollectionUtils.isNotEmpty(exportFiles.get(key))) {
            List<String> s3ExportFilePaths = JsonUtils.convertList(exportFiles.get(key), String.class);
            s3ExportFilePaths.forEach(exportPath -> {
                List<ExportFileConfig> fileConfigs = retFiles.getOrDefault(FilenameUtils.getExtension(exportPath),
                        new ArrayList<>());
                fileConfigs.add(
                        new ExportFileConfig(exportPath.substring(exportPath.indexOf("dropfolder")), exportS3Bucket));
                retFiles.put(FilenameUtils.getExtension(exportPath), fileConfigs);
            });
        }

        return retFiles;
    }

    @VisibleForTesting
    public void setDropBoxProxy(DropBoxProxy dropBoxProxy) {
        this.dropBoxProxy = dropBoxProxy;
    }

    private boolean checkAcxiomFeatureFlag(CDLExternalSystemName extSysName, CustomerSpace customerSpace) {
        ArrayList<CDLExternalSystemName> adPlatforms = new ArrayList<CDLExternalSystemName>(Arrays.asList(CDLExternalSystemName.GoogleAds, CDLExternalSystemName.Facebook, CDLExternalSystemName.LinkedIn));
        boolean isAdPlatform = adPlatforms.contains(extSysName);
        boolean acxiomFlag = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ACXIOM);
        log.info("Setting enableAcxiom as: " + (isAdPlatform && acxiomFlag));

        return isAdPlatform && acxiomFlag;
    }

    private void setOutreachTaskSettings(ExternalIntegrationMessageBody messageBody, CustomerSpace customerSpace) {
        if (messageBody.getTaskType() != null) {
            List<String> taskSettings = WorkflowJobUtils.getOutreachTaskSettingsFromZK(customerSpace);
            String ownerPriority = taskSettings.get(0);
            String defaultOwner = taskSettings.get(1);

            messageBody.setTaskOwnerPriority(ownerPriority);
            messageBody.setTaskDefaultOwner(defaultOwner);
        }
    }
}
