package com.latticeengines.apps.cdl.tray.entitymgr.impl;

import java.util.Date;
import java.util.UUID;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.apps.cdl.tray.entitymgr.TrayConnectorTestEntityMgr;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTest;

public class TrayConnectorTestEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private TrayConnectorTestEntityMgr trayConnectorTestEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testCrud() {
        TrayConnectorTest test1 = new TrayConnectorTest();
        test1.setCDLExternalSystemName(CDLExternalSystemName.Facebook);
        test1.setStartTime(new Date());
        test1.setCDLExternalSystemName(CDLExternalSystemName.Facebook);
        test1.setTestScenario("1");
        String workflowRequestId = UUID.randomUUID().toString();
        test1.setWorkflowRequestId(workflowRequestId);
        test1.setTestState(DataIntegrationEventType.WorkflowSubmitted);
        trayConnectorTestEntityMgr.create(test1);

        TrayConnectorTest newTest = new TrayConnectorTest();
        newTest.setWorkflowRequestId(workflowRequestId);
        newTest.setTestState(DataIntegrationEventType.Completed);
        TrayConnectorTest test = trayConnectorTestEntityMgr.updateTrayConnectorTest(newTest);
        Assert.assertEquals(test.getTestState(), DataIntegrationEventType.Completed);

        test = trayConnectorTestEntityMgr.findByWorkflowRequestId(workflowRequestId);
        Assert.assertEquals(test.getTestState(), DataIntegrationEventType.Completed);

        trayConnectorTestEntityMgr.deleteByWorkflowRequestId(workflowRequestId);
        test = trayConnectorTestEntityMgr.findByWorkflowRequestId(workflowRequestId);
        Assert.assertNull(test);
    }

}
