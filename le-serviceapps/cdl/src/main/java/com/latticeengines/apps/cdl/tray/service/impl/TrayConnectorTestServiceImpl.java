package com.latticeengines.apps.cdl.tray.service.impl;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.latticeengines.apps.cdl.tray.entitymgr.TrayConnectorTestEntityMgr;
import com.latticeengines.apps.cdl.tray.service.TrayConnectorTestService;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.aws.sns.SNSService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.AudienceEventDetail;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.ExportFileConfig;
import com.latticeengines.domain.exposed.cdl.ExternalIntegrationMessageBody;
import com.latticeengines.domain.exposed.cdl.MessageType;
import com.latticeengines.domain.exposed.cdl.ProgressEventDetail;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTest;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTestMetadata;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTestMetadata.TriggerConfig;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTestMetadata.TriggerMetadata;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTestMetadata.ValidationConfig;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTestMetadata.ValidationMetadata;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTestResultType;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;

@Component("trayConnectorTestService")
public class TrayConnectorTestServiceImpl implements TrayConnectorTestService {

    private static final Logger log = LoggerFactory.getLogger(TrayConnectorTestServiceImpl.class);

    private static final String CDL_TRAY_TEST_VERIFICATION_END_POINT = "/cdl/tray/test/verify";
    private static final String PATH_TEMPLATE = "le-tray-testframework/connectors/%s/test-scenarios/%s/input.json";
    private static final String TEST_FILE_PATH_TEMPLATE = "tray-test/%s/%s/%s";

    private static final String CSV = "csv";
    private static final String URL = "Url";
    private static final String MAPPING = "Mapping";
    private static final String QA_MAP = "QA_MAP";
    private static final String PROD_MAP = "PROD_MAP";

    private static final Map<String, Map<CDLExternalSystemName, String>> SOLUTION_INSTANCE_ID_MAP = new HashMap<>();
    private static final Map<CDLExternalSystemName, String>> SOLUTION_INSTANCE_ID_QA_MAP = new HashMap<>();
    private static final Map<CDLExternalSystemName, String>> SOLUTION_INSTANCE_ID_PROD_MAP = new HashMap<>();

    static {
        SOLUTION_INSTANCE_ID_QA_MAP.put(CDLExternalSystemName.Facebook, "d01b9af1-e773-42a9-b660-f42378cbc747");
        SOLUTION_INSTANCE_ID_QA_MAP.put(CDLExternalSystemName.GoogleAds, "e089a0e4-9b89-45ff-ac9d-d75e643ac212");
        SOLUTION_INSTANCE_ID_QA_MAP.put(CDLExternalSystemName.LinkedIn, "add0a7c7-4342-42cb-a726-63e488aa23a3");
        SOLUTION_INSTANCE_ID_QA_MAP.put(CDLExternalSystemName.Outreach, "8a290053-5a9c-418f-a89e-24ebf82a4ff2");
        SOLUTION_INSTANCE_ID_QA_MAP.put(CDLExternalSystemName.Marketo, "64497731-1be1-4b25-8830-56fcb1ffcf82");

        SOLUTION_INSTANCE_ID_PROD_MAP.put(CDLExternalSystemName.Facebook, "07a65fa5-a57c-4e06-a635-9f4f9cf07bb8");
        SOLUTION_INSTANCE_ID_PROD_MAP.put(CDLExternalSystemName.GoogleAds, "3ad98389-e8a8-4dbd-8514-7eddca59fecc");
        SOLUTION_INSTANCE_ID_PROD_MAP.put(CDLExternalSystemName.LinkedIn, "a3d54317-2821-46fe-afaf-3ce7e9b38741");
        SOLUTION_INSTANCE_ID_PROD_MAP.put(CDLExternalSystemName.Outreach, "654a3337-9636-4aa9-b11c-eb1eb7b88b39");
        SOLUTION_INSTANCE_ID_PROD_MAP.put(CDLExternalSystemName.Marketo, "6258465e-1d57-4c0c-8d7f-bfa070c063fb");

        SOLUTION_INSTANCE_ID_MAP.put(QA_MAP, SOLUTION_INSTANCE_ID_QA_MAP);
        SOLUTION_INSTANCE_ID_MAP.put(PROD_MAP, SOLUTION_INSTANCE_ID_PROD_MAP);
    }

    @Inject
    private TrayConnectorTestEntityMgr trayConnectorTestEntityMgr;

    @Inject
    private SNSService snsService;

    @Inject
    private S3Service s3Service;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private DynamoItemService dynamoItemService;

    @Value("${cdl.campaign.integration.session.context.ttl}")
    private long sessionContextTTLinSec;

    @Value("${cdl.campaign.integration.session.context.dynamo.table}")
    private String integrationSessionContextTable;

    @Value("${common.microservice.url}")
    private String microserviceHostPort;

    @Value("${aws.data.integration.exportdata.topic}")
    private String exportDataTopic;

    @Value("${aws.customer.export.s3.bucket}")
    private String exportS3Bucket;

    @Value("${cdl.tray.test.data.bucket}")
    private String trayTestDataBucket;

    @Value("${cdl.tray.test.solutionInstanceMap}")
    private String trayTestSolutionInstanceMap;

    @Value("${cdl.atlas.export.dropfolder.tag}")
    private String expire30dTag;

    @Value("${cdl.atlas.export.dropfolder.tag.value}")
    private String expire30dTagValue;

    @Override
    public void triggerTrayConnectorTest(String customerSpace, CDLExternalSystemName externalSystemName, String testScenario) {

        String tenantId = MultiTenantContext.getShortTenantId();
        String objectKey = String.format(PATH_TEMPLATE, externalSystemName, testScenario);
        log.info(String.format("Trigger test for tenant=%s, objectKey=%s", tenantId, objectKey));

        try {
            InputStream is = s3Service.readObjectAsStream(trayTestDataBucket, objectKey);
            TrayConnectorTestMetadata metadata = JsonUtils.deserialize(is, TrayConnectorTestMetadata.class);
            log.info("Retrieved metadata: " + JsonUtils.serialize(metadata));

            String workflowRequestId = UUID.randomUUID().toString();
            log.info("Generated workflowRequestId: " + workflowRequestId);

            publishSessionContext(workflowRequestId);

            TriggerMetadata trigger = metadata.getTrigger();
            ExternalIntegrationMessageBody messageBody = trigger.getMessage();
            TriggerConfig triggerConfig = trigger.getTriggerConfig();

            if (triggerConfig.getGenerateSolutionInstance()) {
                // TODO generate solution instance
            }

            if (triggerConfig.getGenerateExternalAudience()) {
                // TODO generate external audience
            }

            DropBoxSummary dropboxSummary = dropBoxProxy.getDropBox(tenantId);
            messageBody.setTrayTenantId(dropboxSummary.getDropBox());
            log.info("Setting Tray tenant ID: " + dropboxSummary.getDropBox());

            copyInputFiles(messageBody, externalSystemName);

            publishToSnsTopic(externalSystemName, workflowRequestId, messageBody);

            registerTrayConnectorTest(externalSystemName, testScenario, workflowRequestId);
        } catch (Exception e) {
            log.error("Failed to trigger test", e.toString());
        }
    }

    @Override
    public Map<String, Boolean> verifyTrayConnectorTest(List<DataIntegrationStatusMonitorMessage> statuses) {
        log.info("Verifying status messages for Tray test...");

        Map<String, Boolean> statusesUpdate = new HashMap<>();
        statuses.forEach(status -> {
            String statusJson = JsonUtils.serialize(status);
            log.info("Serialized message: " + statusJson);
            statusesUpdate.put(status.getWorkflowRequestId(), handleStatus(status));
        });

        return statusesUpdate;
    }

    @Override
    public List<TrayConnectorTest> findUnfinishedTests() {
        return trayConnectorTestEntityMgr.findUnfinishedTests();
    }

    @Override
    public void cancelTrayTestByWorkflowReqId(String workflowRequestId) {
        TrayConnectorTest test = trayConnectorTestEntityMgr.findByWorkflowRequestId(workflowRequestId);
        log.info("Cancelling test with Workflow Request ID: " + workflowRequestId);

        try {
            test.setTestResult(TrayConnectorTestResultType.Cancelled);
            test.setEndTime(new Date());
            trayConnectorTestEntityMgr.updateTrayConnectorTest(test);
        } catch (Exception e) {
            log.error("Failed to cancel test", e.toString());
        }
    }

    @Override
    public boolean isAdPlatform(TrayConnectorTest test) {
        CDLExternalSystemName externalSystemName = test.getExternalSystemName();
        return CDLExternalSystemName.AD_PLATFORMS.contains(externalSystemName);
    }

    @Override
    public boolean isLiveramp(TrayConnectorTest test) {
        CDLExternalSystemName externalSystemName = test.getExternalSystemName();
        return CDLExternalSystemName.LIVERAMP.contains(externalSystemName);
    }

    private Boolean handleStatus(DataIntegrationStatusMonitorMessage status) {
        try {
            String workflowRequestId = status.getWorkflowRequestId();
            log.info("Workflow Request ID: " + workflowRequestId);

            TrayConnectorTest test = trayConnectorTestEntityMgr.findByWorkflowRequestId(workflowRequestId);
            String objectKey = String.format(PATH_TEMPLATE, test.getExternalSystemName(), test.getTestScenario());
            log.info("Retrieving test metadata with objectKey: " + objectKey);

            InputStream is = s3Service.readObjectAsStream(trayTestDataBucket, objectKey);
            TrayConnectorTestMetadata metadata = JsonUtils.deserialize(is, TrayConnectorTestMetadata.class);
            log.info("Retrieved metadata: " + JsonUtils.serialize(metadata));

            ValidationMetadata validation = metadata.getValidation();
            List<DataIntegrationStatusMonitorMessage> validationMessages = validation.getMessages();
            updateTest(status, test, validationMessages);

            ValidationConfig validationConfig = validation.getValidationConfig();
            cleanUp(validationConfig, test);
            return true;
        } catch (Exception e) {
            log.error("Failed to process status", e.toString());
            return false;
        }
    }

    private void updateTest(DataIntegrationStatusMonitorMessage status, TrayConnectorTest test,
            List<DataIntegrationStatusMonitorMessage> validationMessages) {

        if (test.getTestResult() != null) {
            log.warn("This test is already completed with result " + test.getTestResult());
            return;
        }

        if (!DataIntegrationEventType.canTransit(test.getTestState(),
                DataIntegrationEventType.valueOf(status.getEventType()))) {
            log.warn(String.format("State can't change from % to %", test.getTestState().toString(), status.getEventType()));
            return;
        }

        if (isExpectedStatus(status, validationMessages)) {
            test.setTestState(DataIntegrationEventType.valueOf(status.getEventType()));
            if (isLastStatus(status, test)) {
                log.info("Received last expected status update. Setting test result to Succeeded");
                test.setTestResult(TrayConnectorTestResultType.Succeeded);
                test.setEndTime(new Date());
            }
        } else {
            log.warn("Received unexpected status update. Setting test result to Failed");
            test.setTestResult(TrayConnectorTestResultType.Failed);
            test.setEndTime(new Date());
        }

        trayConnectorTestEntityMgr.updateTrayConnectorTest(test);
    }

    private boolean isExpectedStatus(DataIntegrationStatusMonitorMessage status,
            List<DataIntegrationStatusMonitorMessage> validationMessages) {

        Map<String, DataIntegrationStatusMonitorMessage> messageMap = new HashMap<>();
        for (DataIntegrationStatusMonitorMessage message : validationMessages) {
            messageMap.put(message.getEventType(), message);
        }

        DataIntegrationEventType currEventType = DataIntegrationEventType.valueOf(status.getEventType());

        switch (currEventType) {
            case ExportStart:
                return verifyExportStartMessage(status, messageMap.get(status.getEventType()));

            case Initiated:
                return verifyInitiatedMessage(status, messageMap.get(status.getEventType()));

            case Completed:
                return verifyCompletedMessage(status, messageMap.get(status.getEventType()));

            case AudienceSizeUpdate:
                return verifyAudienceSizeUpdateMessage(status, messageMap.get(status.getEventType()));

            default:
                return false;
        }
    }

    private boolean verifyExportStartMessage(DataIntegrationStatusMonitorMessage status, DataIntegrationStatusMonitorMessage expected) {
        log.info("Verifying ExportStart message");
        return (expected != null) && (expected.getMessageType().equals(MessageType.Event.toString()));
    }

    private boolean verifyInitiatedMessage(DataIntegrationStatusMonitorMessage status, DataIntegrationStatusMonitorMessage expected) {
        log.info("Verifying Initiated message");
        return (expected != null) && (expected.getMessageType().equals(MessageType.Event.toString()));
    }

    private boolean verifyCompletedMessage(DataIntegrationStatusMonitorMessage status, DataIntegrationStatusMonitorMessage expected) {
        log.info("Verifying Completed message");
        if (expected == null) {
            return false;
        }

        ProgressEventDetail eventDetail = (ProgressEventDetail) status.getEventDetail();
        Long totalRecords = eventDetail.getTotalRecordsSubmitted();
        Long recordsProcessed = eventDetail.getProcessed();
        log.info(String.format("Test result: totalRecords=%s, recordsProcessed=%s", totalRecords.toString(), recordsProcessed.toString()));

        ProgressEventDetail expectedEventDetail = (ProgressEventDetail) expected.getEventDetail();
        Long expectedTotalRecords = expectedEventDetail.getTotalRecordsSubmitted();
        Long expectedRecordsProcessed = expectedEventDetail.getProcessed();

        return (totalRecords.equals(expectedTotalRecords)) && (recordsProcessed.equals(expectedRecordsProcessed));
    }

    private boolean verifyAudienceSizeUpdateMessage(DataIntegrationStatusMonitorMessage status, DataIntegrationStatusMonitorMessage expected) {
        log.info("Verifying AudienceSizeUpdate message");
        if (expected == null) {
            return false;
        }

        AudienceEventDetail eventDetail = (AudienceEventDetail) status.getEventDetail();
        Long audienceSize = eventDetail.getAudienceSize();
        log.info(String.format("Test result: audienceSize=%s", audienceSize.toString()));

        // We sometimes receive 0 as an audienceSize after matching
        return audienceSize != null;
    }

    private void cleanUp(ValidationConfig validationConfig, TrayConnectorTest test) {
        if (test.getTestResult() == null) {
            return;
        }

        if (validationConfig.getDeleteSolutionInstance()) {
            // TODO delete solution instance
        }

        if (validationConfig.getDeleteExternalAudience()) {
            // TODO delete external audience
        }
    }

    private boolean isLastStatus(DataIntegrationStatusMonitorMessage status, TrayConnectorTest test) {
        CDLExternalSystemName externalSystemName = test.getExternalSystemName();
        if (CDLExternalSystemName.AD_PLATFORMS.contains(externalSystemName)) {
            return status.getEventType().equals(DataIntegrationEventType.AudienceSizeUpdate.toString());
        } else {
            return status.getEventType().equals(DataIntegrationEventType.Completed.toString());
        }
    }

    private void registerTrayConnectorTest(CDLExternalSystemName externalSystemName, String testScenario,
            String workflowRequestId) {
        TrayConnectorTest test = new TrayConnectorTest();
        test.setStartTime(new Date());
        test.setCDLExternalSystemName(externalSystemName);
        test.setTenant(MultiTenantContext.getTenant());
        test.setTestScenario(testScenario);
        test.setTestState(DataIntegrationEventType.WorkflowSubmitted);
        test.setWorkflowRequestId(workflowRequestId);
        trayConnectorTestEntityMgr.create(test);
    }

    private void publishSessionContext(String workflowRequestId) {
        log.info(String.format("Publish to DynamoDB %s with workflowRequestId %s and Url %s",
                integrationSessionContextTable, workflowRequestId,
                microserviceHostPort + CDL_TRAY_TEST_VERIFICATION_END_POINT));
        dynamoItemService.putItem(integrationSessionContextTable, getItem(workflowRequestId));
    }

    private Item getItem(String workflowRequestId) {
        Map<String, Object> session = new HashMap<String, Object>();
        session.put(URL, microserviceHostPort + CDL_TRAY_TEST_VERIFICATION_END_POINT);
        session.put(MAPPING, "");
        return new Item().withPrimaryKey("WorkflowId", workflowRequestId)
                .withLong("TTL", System.currentTimeMillis() / 1000 + sessionContextTTLinSec)
                .withString("Session", JsonUtils.serialize(session));
    }

    private PublishResult publishToSnsTopic(CDLExternalSystemName externalSystemName,
            String workflowRequestId, ExternalIntegrationMessageBody messageBody) {

        messageBody.setWorkflowRequestId(workflowRequestId);

        String solutionInstanceId = SOLUTION_INSTANCE_ID_MAP.get(trayTestSolutionInstanceMap).get(externalSystemName);
        messageBody.setSolutionInstanceId(solutionInstanceId);

        // TODO may need to set solutionInstanceId and externalAudienceId
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
            log.error("Failed to publish to SNS ", e.toString());
            return null;
        }
    }

    private void copyInputFiles(ExternalIntegrationMessageBody messageBody, CDLExternalSystemName externalSystemName) {
        Map<String, List<ExportFileConfig>> sourceFiles = messageBody.getSourceFiles();
        Map<String, List<ExportFileConfig>> deleteFiles = messageBody.getDeleteFiles();
        String tenantId = messageBody.getTrayTenantId();

        if (sourceFiles != null) {
            List<ExportFileConfig> newFileList = getNewFileList(sourceFiles, tenantId, externalSystemName);
            sourceFiles.put(CSV, newFileList);
            messageBody.setSourceFiles(sourceFiles);
        }
        if (deleteFiles != null) {
            List<ExportFileConfig> newFileList = getNewFileList(deleteFiles, tenantId, externalSystemName);
            deleteFiles.put(CSV, newFileList);
            messageBody.setDeleteFiles(deleteFiles);
        }
    }

    private List<ExportFileConfig> getNewFileList(Map<String, List<ExportFileConfig>> inputFile, String tenantId, CDLExternalSystemName externalSystemName) {
        String testObjectKey = inputFile.get(CSV).get(0).getObjectPath();
        String fileName = testObjectKey.substring(testObjectKey.lastIndexOf("/")+1);
        String objectKey = String.format(TEST_FILE_PATH_TEMPLATE, tenantId, externalSystemName, fileName);
        s3Service.copyObject(trayTestDataBucket, testObjectKey, exportS3Bucket, objectKey);
        tagS3Expiration(exportS3Bucket, objectKey);
        log.info("Copied tset input file in " + objectKey);

        ExportFileConfig exportFileConfig = new ExportFileConfig();
        exportFileConfig.setObjectPath(objectKey);
        exportFileConfig.setBucketName(exportS3Bucket);

        List<ExportFileConfig> newFileList = new ArrayList<>();
        newFileList.add(exportFileConfig);

        return newFileList;
    }

    private void tagS3Expiration(String bucket, String key) {
        log.info("Tagging the copied input file to expire in 30 days");
        try {
            s3Service.addTagToObject(bucket, key, expire30dTag, expire30dTagValue);
            log.info(String.format("Tagged %s to expire in 30 days", key));
        } catch (Exception e) {
            log.error(String.format("Failed to tag %s to expire in 30 days", key));
        }
    }
}
