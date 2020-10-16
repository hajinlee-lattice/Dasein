package com.latticeengines.apps.cdl.tray.entitymgr.impl;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.apps.cdl.tray.entitymgr.TrayConnectorTestEntityMgr;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTest;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTestResultType;

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
        test1.setTestScenario("1");
        String workflowRequestId = UUID.randomUUID().toString();
        test1.setWorkflowRequestId(workflowRequestId);
        test1.setTestState(DataIntegrationEventType.WorkflowSubmitted);
        trayConnectorTestEntityMgr.create(test1);

        RetryTemplate retry = RetryUtils.getRetryTemplate(5, //
                Collections.singleton(AssertionError.class), null);

        retry.execute(context -> {
            TrayConnectorTest createdTest = trayConnectorTestEntityMgr.findByWorkflowRequestId(workflowRequestId);
            Assert.assertNotNull(createdTest);
            return true;
        });

        TrayConnectorTest newTest = new TrayConnectorTest();
        newTest.setWorkflowRequestId(workflowRequestId);
        newTest.setTestState(DataIntegrationEventType.Completed);
        TrayConnectorTest test = trayConnectorTestEntityMgr.updateTrayConnectorTest(newTest);
        Assert.assertEquals(test.getTestState(), DataIntegrationEventType.Completed);

        retry.execute(context -> {
            TrayConnectorTest currTest = trayConnectorTestEntityMgr.findByWorkflowRequestId(workflowRequestId);
            Assert.assertEquals(currTest.getTestState(), DataIntegrationEventType.Completed);
            return true;
        });

        trayConnectorTestEntityMgr.deleteByWorkflowRequestId(workflowRequestId);
        retry.execute(context -> {
            TrayConnectorTest currTest = trayConnectorTestEntityMgr.findByWorkflowRequestId(workflowRequestId);
            Assert.assertNull(currTest);
            return true;
        });
    }

    @Test(groups = "functional")
    public void testFindUnfinishedTests() {
        List<TrayConnectorTest> hangingTests = trayConnectorTestEntityMgr.findUnfinishedTests();
        if (hangingTests.size() != 0) {
            hangingTests.forEach(test -> {
                trayConnectorTestEntityMgr.deleteByWorkflowRequestId(test.getWorkflowRequestId());
            });
        }

        TrayConnectorTest test1 = new TrayConnectorTest();
        test1.setCDLExternalSystemName(CDLExternalSystemName.Facebook);
        test1.setTestState(DataIntegrationEventType.WorkflowSubmitted);
        String workflowRequestId1 = UUID.randomUUID().toString();
        test1.setWorkflowRequestId(workflowRequestId1);
        test1.setStartTime(new Date());
        trayConnectorTestEntityMgr.create(test1);

        TrayConnectorTest test2 = new TrayConnectorTest();
        test2.setCDLExternalSystemName(CDLExternalSystemName.GoogleAds);
        test2.setTestState(DataIntegrationEventType.WorkflowSubmitted);
        String workflowRequestId2 = UUID.randomUUID().toString();
        test2.setWorkflowRequestId(workflowRequestId2);
        test2.setStartTime(new Date());
        test2.setTestResult(TrayConnectorTestResultType.Succeeded);
        trayConnectorTestEntityMgr.create(test2);

        List<TrayConnectorTest> unfinishedTests = trayConnectorTestEntityMgr.findUnfinishedTests();
        Assert.assertEquals(unfinishedTests.size(), 1);

        // Clean up from here
        trayConnectorTestEntityMgr.deleteByWorkflowRequestId(workflowRequestId1);
        trayConnectorTestEntityMgr.deleteByWorkflowRequestId(workflowRequestId2);

        RetryTemplate retry = RetryUtils.getRetryTemplate(5, //
                Collections.singleton(AssertionError.class), null);

        retry.execute(context -> {
            TrayConnectorTest deletedTest1 = trayConnectorTestEntityMgr.findByWorkflowRequestId(workflowRequestId1);
            TrayConnectorTest deletedTest2 = trayConnectorTestEntityMgr.findByWorkflowRequestId(workflowRequestId2);
            Assert.assertNull(deletedTest1);
            Assert.assertNull(deletedTest2);
            return true;
        });
    }

}
