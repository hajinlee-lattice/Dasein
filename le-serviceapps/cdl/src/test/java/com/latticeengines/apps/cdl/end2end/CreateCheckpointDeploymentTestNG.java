package com.latticeengines.apps.cdl.end2end;

import static org.testng.Assert.assertEquals;

import java.util.Collections;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.FailingStep;



public class CreateCheckpointDeploymentTestNG  extends CDLEnd2EndDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(CreateCheckpointDeploymentTestNG.class);

    static final String CHECK_POINT = "process1";

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        log.info("In runTest()");

        // Construct a ProcessAnalyzeRequest that includes a failure.
        ProcessAnalyzeRequest processAnalyzeRequest = new ProcessAnalyzeRequest();
        FailingStep failingStep = new FailingStep();
        Integer failStep = 1;
        failingStep.setSeq(failStep);
        log.info("Will fail after step " + failStep);
        processAnalyzeRequest.setFailingStep(failingStep);

        try {
            importData();
            processAnalyze(processAnalyzeRequest);
            verifyProcess();
        } finally {
            testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
            if (isLocalEnvironment()) {
                saveCheckpoint(CHECK_POINT);
            }
        }
    }

    private void importData() throws Exception {
        log.info("In importData()");

        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        mockCSVImport(BusinessEntity.Account, 1, "Account");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Contact, 1, "Contact");
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    @Override
    void processAnalyze(ProcessAnalyzeRequest request) {
        log.info("Start processing and analyzing ...");
        ApplicationId appId = cdlProxy.processAnalyze(mainTestTenant.getId(), request);
        processAnalyzeAppId = appId.toString();
        log.info("processAnalyzeAppId=" + processAnalyzeAppId);
        com.latticeengines.domain.exposed.workflow.JobStatus completedStatus = waitForWorkflowStatus(appId.toString(),
                false);
        log.info("JobStatus is: " + completedStatus.getName());
        assertEquals(completedStatus, com.latticeengines.domain.exposed.workflow.JobStatus.FAILED);
    }

    private void verifyProcess() {
        log.info("In verifyProcess()");

    }
}
