package com.latticeengines.apps.cdl.service.impl;

import static org.testng.Assert.assertNotNull;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMessageEntityMgr;
import com.latticeengines.apps.cdl.service.DataIntegrationStatusMonitoringService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMessage;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.MessageType;

public class DataIntegrationStatusMonitoringServiceImplTestNG extends CDLFunctionalTestNGBase {

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
        String eventId = UUID.randomUUID().toString();
        DataIntegrationStatusMonitorMessage statusMessage = createDefaultStatusMessage(eventId,
                DataIntegrationEventType.WORKFLOW_SUBMITTED.toString());
        dataIntegrationStatusMonitoringService.createOrUpdateStatus(statusMessage);

        DataIntegrationStatusMonitor statusMonitor = dataIntegrationStatusMonitoringService
                .getStatus(statusMessage.getWorkflowRequestId());

        Assert.assertNotNull(statusMonitor);
    }

    private DataIntegrationStatusMonitorMessage createDefaultStatusMessage(String workflowRequestId,
            String eventType) {
        DataIntegrationStatusMonitorMessage statusMessage = new DataIntegrationStatusMonitorMessage();
        statusMessage.setTenantId(mainTestTenant.getPid());
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
        DataIntegrationStatusMonitorMessage statusMessage = createDefaultStatusMessage(workflowRequestId,
                DataIntegrationEventType.WORKFLOW_STARTED.toString());
        
        boolean exceptionThrown = false;
        try {
            dataIntegrationStatusMonitoringService.createOrUpdateStatus(statusMessage);
        } catch (Exception e) {
            exceptionThrown = true;
        }
        Assert.assertTrue(exceptionThrown);

    }

}
