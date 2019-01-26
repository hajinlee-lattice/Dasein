package com.latticeengines.apps.cdl.service.impl;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMessageEntityMgr;
import com.latticeengines.apps.cdl.service.DataIntegrationStatusMonitoringService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMessage;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.MessageType;

public class DataIntegrationStatusMonitoringServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DataIntegrationStatusMonitoringServiceImplTestNG.class);

    @Inject
    DataIntegrationStatusMonitoringService dataIntegrationStatusMonitoringService;

    @Inject
    DataIntegrationStatusMessageEntityMgr dataIntegrationStatusMessageEntityMgr;

    private String ENTITY_NAME = "PLAY";
    private String ENTITY_ID = "launch" + UUID.randomUUID().toString();
    private String SOURCE_FILE = "s3://";
    private String EXTERNAL_SYSTEM_ID = "crm_" + UUID.randomUUID().toString();

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testCreateAndGet() {
        String workflowRequestId = UUID.randomUUID().toString();
        DataIntegrationStatusMonitorMessage statusMessage = createDefaultStatusMessage(workflowRequestId,
                DataIntegrationEventType.WORKFLOW_SUBMITTED.toString());
        dataIntegrationStatusMonitoringService.createOrUpdateStatus(statusMessage);

        DataIntegrationStatusMonitor statusMonitor = dataIntegrationStatusMonitoringService
                .getStatus(statusMessage.getWorkflowRequestId());

        Assert.assertNotNull(statusMonitor);
        Assert.assertEquals(DataIntegrationEventType.WORKFLOW_SUBMITTED.toString(), statusMonitor.getStatus());
        Assert.assertNotNull(statusMonitor.getEventSubmittedTime());
    }

    private DataIntegrationStatusMonitorMessage createDefaultStatusMessage(String workflowRequestId,
            String eventType) {
        DataIntegrationStatusMonitorMessage statusMessage = new DataIntegrationStatusMonitorMessage();
        statusMessage.setTenantId(mainTestTenant.getId());
        statusMessage.setWorkflowRequestId(workflowRequestId);
        statusMessage.setEntityId(ENTITY_NAME);
        statusMessage.setEntityName(ENTITY_NAME);
        statusMessage.setExternalSystemId(EXTERNAL_SYSTEM_ID);
        statusMessage.setOperation("export");
        statusMessage.setMessageType(MessageType.EVENT.toString());
        statusMessage.setMessage("This workflow has been submitted");
        statusMessage.setEventType(eventType);
        statusMessage.setEventTime(new Date());
        statusMessage.setSourceFile(SOURCE_FILE);
        return statusMessage;
    }
    
    @Test(groups = "functional")
    public void testCreateWithIncorrectOrder() {
        String workflowRequestId = UUID.randomUUID().toString();
        DataIntegrationStatusMonitorMessage statusMessage = new DataIntegrationStatusMonitorMessage();
        statusMessage.setWorkflowRequestId(workflowRequestId);
        statusMessage.setEventType(DataIntegrationEventType.WORKFLOW_STARTED.toString());
        statusMessage.setEventTime(new Date());
        statusMessage.setMessageType(MessageType.EVENT.toString());
        statusMessage.setMessage("test");

        Boolean exceptionThrown = false;
        try {
            dataIntegrationStatusMonitoringService.createOrUpdateStatus(statusMessage);
        } catch (Exception e) {
            log.info("Caught exception creating status monitor: " + e.getMessage());
            exceptionThrown = true;
        }

        Assert.assertTrue(exceptionThrown);
    }

    @Test(groups = "functional")
    public void testUpdateWithCorrectOrder() {
        String workflowRequestId = UUID.randomUUID().toString();
        DataIntegrationStatusMonitorMessage createStatusMonitorMessage = createDefaultStatusMessage(workflowRequestId,
                DataIntegrationEventType.WORKFLOW_SUBMITTED.toString());

        dataIntegrationStatusMonitoringService.createOrUpdateStatus(createStatusMonitorMessage);
        DataIntegrationStatusMonitor statusMonitor = dataIntegrationStatusMonitoringService
                .getStatus(workflowRequestId);

        Assert.assertNotNull(statusMonitor);

        DataIntegrationStatusMonitorMessage updateStatusMonitorMessage = new DataIntegrationStatusMonitorMessage();
        updateStatusMonitorMessage.setWorkflowRequestId(workflowRequestId);
        updateStatusMonitorMessage.setEventType(DataIntegrationEventType.WORKFLOW_STARTED.toString());
        updateStatusMonitorMessage.setEventTime(new Date());
        updateStatusMonitorMessage.setMessageType(MessageType.EVENT.toString());
        updateStatusMonitorMessage.setMessage("test");

        dataIntegrationStatusMonitoringService.createOrUpdateStatus(updateStatusMonitorMessage);

        statusMonitor = dataIntegrationStatusMonitoringService
                .getStatus(updateStatusMonitorMessage.getWorkflowRequestId());

        Assert.assertNotNull(statusMonitor);
        Assert.assertNotNull(statusMonitor.getTenant());
        Assert.assertNotNull(statusMonitor.getEventStartedTime());
        Assert.assertNotNull(statusMonitor.getEventSubmittedTime());
        Assert.assertEquals(DataIntegrationEventType.WORKFLOW_STARTED.toString(), statusMonitor.getStatus());

        List<DataIntegrationStatusMessage> messages = dataIntegrationStatusMessageEntityMgr
                .getAllStatusMessages(statusMonitor.getPid());

        Assert.assertNotNull(messages);
        Assert.assertEquals(messages.size(), 2);
    }

    @Test(groups = "functional")
    public void testUpdateWithIncorrectOrder() {
        String workflowRequestId = UUID.randomUUID().toString();
        DataIntegrationStatusMonitorMessage createStatusMonitorMessage = createDefaultStatusMessage(workflowRequestId,
                DataIntegrationEventType.WORKFLOW_SUBMITTED.toString());

        dataIntegrationStatusMonitoringService.createOrUpdateStatus(createStatusMonitorMessage);
        DataIntegrationStatusMonitor statusMonitor = dataIntegrationStatusMonitoringService
                .getStatus(createStatusMonitorMessage.getWorkflowRequestId());

        Assert.assertNotNull(statusMonitor);

        DataIntegrationStatusMonitorMessage updateStatusMonitorMessage = new DataIntegrationStatusMonitorMessage();
        updateStatusMonitorMessage.setWorkflowRequestId(workflowRequestId);
        updateStatusMonitorMessage.setEventType(DataIntegrationEventType.WORKFLOW_COMPLETED.toString());
        updateStatusMonitorMessage.setEventTime(new Date());
        updateStatusMonitorMessage.setMessageType(MessageType.EVENT.toString());
        updateStatusMonitorMessage.setMessage("test");

        dataIntegrationStatusMonitoringService.createOrUpdateStatus(updateStatusMonitorMessage);

        statusMonitor = dataIntegrationStatusMonitoringService
                .getStatus(updateStatusMonitorMessage.getWorkflowRequestId());

        Assert.assertNotNull(statusMonitor);
        Assert.assertNotNull(statusMonitor.getEventSubmittedTime());
        Assert.assertNotNull(statusMonitor.getEventCompletedTime());
        Assert.assertEquals(DataIntegrationEventType.WORKFLOW_COMPLETED.toString(), statusMonitor.getStatus());

        updateStatusMonitorMessage = new DataIntegrationStatusMonitorMessage();
        updateStatusMonitorMessage.setWorkflowRequestId(workflowRequestId);
        updateStatusMonitorMessage.setEventType(DataIntegrationEventType.WORKFLOW_STARTED.toString());
        updateStatusMonitorMessage.setEventTime(new Date());
        updateStatusMonitorMessage.setMessageType(MessageType.EVENT.toString());
        updateStatusMonitorMessage.setMessage("test");

        dataIntegrationStatusMonitoringService.createOrUpdateStatus(updateStatusMonitorMessage);

        statusMonitor = dataIntegrationStatusMonitoringService.getStatus(workflowRequestId);

        Assert.assertNotNull(statusMonitor);
        Assert.assertNotNull(statusMonitor.getEventSubmittedTime());
        Assert.assertNotNull(statusMonitor.getEventStartedTime());
        Assert.assertNotNull(statusMonitor.getEventCompletedTime());
        Assert.assertEquals(DataIntegrationEventType.WORKFLOW_COMPLETED.toString(), statusMonitor.getStatus());

        List<DataIntegrationStatusMessage> messages = dataIntegrationStatusMessageEntityMgr
                .getAllStatusMessages(statusMonitor.getPid());

        Assert.assertNotNull(messages);
        Assert.assertEquals(messages.size(), 3);

    }

}
