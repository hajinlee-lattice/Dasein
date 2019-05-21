package com.latticeengines.apps.cdl.end2end;

import java.util.Collections;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class ProcessingReportDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ProcessingReportDeploymentTestNG.class);

    @Inject
    private WorkflowProxy workflowProxy;

    @BeforeClass(groups = "end2end")
    public void setup() throws Exception {
        setupEnd2EndTestEnvironmentWithExistingTenant("JoyTestWarning");
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
    }

    @Test(groups = "end2end")
    public void testMain() {
        importData(BusinessEntity.Account, "Account_1_900.csv", "DefaultSystem_AccountData");
        String applicationId = cdlProxy.processAnalyze(mainTestTenant.getId(), null).toString();
        waitForWorkflowStatus(applicationId, false);
        Job workflowJob = workflowProxy.getWorkflowJobFromApplicationId(applicationId, mainCustomerSpace);
        log.info("reports is :" + JsonUtils.serialize(workflowJob.getReports()));
        Assert.assertTrue(JsonUtils.serialize(workflowJob.getReports()).contains("this warning message from tenant JoyTestWarning, just for testing."));
    }

    @AfterClass(groups = "end2end")
    public void cleanup() {
        testBed.cleanupDlZk();
        testBed.cleanupPlsHdfs();
        testBed.deleteTenant(mainTestTenant);
    }
}
