package com.latticeengines.apps.cdl.tray.service.impl;

import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
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
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.ExternalIntegrationMessageBody;
import com.latticeengines.domain.exposed.cdl.tray.TestState;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTest;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTestMetadata;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTestMetadata.TriggerConfig;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTestMetadata.TriggerMetadata;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;

@Component("trayConnectorTestService")
public class TrayConnectorTestServiceImpl implements TrayConnectorTestService {

    private static final Logger log = LoggerFactory.getLogger(TrayConnectorTestServiceImpl.class);

    @Inject
    private TrayConnectorTestEntityMgr trayConnectorTestEntityMgr;

    @Inject
    private SNSService snsService;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private S3Service s3Service;

    @Inject
    private DynamoItemService dynamoItemService;

    private static final String CDL_TRAY_TEST_VERIFICATION_END_POINT = "/cdl/tray/test/verify";

    @Value("${cdl.campaign.integration.session.context.ttl}")
    private long sessionContextTTL;

    @Value("${cdl.campaign.integration.session.context.dynamo.table}")
    private String integrationSessionContextTable;

    @Value("${common.microservice.url}")
    private String microserviceHostPort;

    @Value("${aws.data.integration.exportdata.topic}")
    private String exportDataTopic;

    @Value("${aws.customer.export.s3.bucket}")
    private String exportS3Bucket;

    @Value("${cdl.tray.test.metadata.bucket}")
    private String trayTestMetadataBucket;

    private String pathTemplate = "connectors/%s/test-scenarios/%s/input.json";

    @Override
    public void triggerTrayConnectorTest(CDLExternalSystemName externalSystemName, String testScenario) {

        String customerSpace = MultiTenantContext.getShortTenantId();
        String objectKey = String.format(pathTemplate, externalSystemName, testScenario);
        log.info(String.format("Trigger test for customerSpace=%s, objectKey=%s", customerSpace, objectKey));

        InputStream is = s3Service.readObjectAsStream(trayTestMetadataBucket, objectKey);
        TrayConnectorTestMetadata metadata = JsonUtils.deserialize(is, TrayConnectorTestMetadata.class);

        String workflowRequestId = UUID.randomUUID().toString();
        publishSessionContext(workflowRequestId);

        TriggerMetadata trigger = metadata.getTrigger();
        ExternalIntegrationMessageBody message = trigger.getMessage();
        TriggerConfig triggerConfig = trigger.getTriggerConfig();
        // TODO generate solution instnace
        if (triggerConfig.getGenerateSolutionInstance()) {

        }
        // TODO generate external audience
        if (triggerConfig.getGenerateExternalAudience()) {

        }

        publishToSnsTopic(customerSpace, workflowRequestId, message);

        registerTrayConnectorTest(externalSystemName, testScenario, workflowRequestId);
    }

    private void registerTrayConnectorTest(CDLExternalSystemName externalSystemName, String testScenario,
            String workflowRequestId) {
        TrayConnectorTest test = new TrayConnectorTest();
        test.setStartTime(new Date());
        test.setCDLExternalSystemName(externalSystemName);
        test.setTenant(MultiTenantContext.getTenant());
        test.setTestScenario(testScenario);
        test.setTestState(TestState.Started);
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
        session.put("Url", microserviceHostPort + CDL_TRAY_TEST_VERIFICATION_END_POINT);
        session.put("Mapping", "");
        return new Item().withPrimaryKey("WorkflowId", workflowRequestId)
                .withLong("TTL", System.currentTimeMillis() + sessionContextTTL)
                .withString("Session", JsonUtils.serialize(session));
    }

    private PublishResult publishToSnsTopic(String customerSpace, String workflowRequestId,
            ExternalIntegrationMessageBody messageBody) {

        DropBoxSummary dropboxSummary = dropBoxProxy.getDropBox(customerSpace);
        messageBody.setWorkflowRequestId(workflowRequestId);
        messageBody.setTrayTenantId(dropboxSummary.getDropBox());
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
            log.info(e.toString());
            return null;
        }
    }

}
