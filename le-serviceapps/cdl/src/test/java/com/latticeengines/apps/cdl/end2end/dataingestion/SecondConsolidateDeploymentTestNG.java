package com.latticeengines.apps.cdl.end2end.dataingestion;

import static org.testng.Assert.assertEquals;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class SecondConsolidateDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SecondConsolidateDeploymentTestNG.class);

    @Test(groups = "end2end")
    public void testFirstConsolidate() throws Exception {
        resumeCheckpoint("profile1");
        verifyFirstProfileCheckpoint();

        importData();
        consolidate();
        verifyConsolidate();

        verifySecondConsolidateCheckpoint();
    }

    private void importData() throws Exception {
        mockAvroData(300, 200);
        Thread.sleep(2000);
    }

    private void consolidate() {
        log.info("Start consolidating ...");
        ApplicationId appId = cdlProxy.consolidate(mainTestTenant.getId());
        JobStatus completedStatus = waitForWorkflowStatus(appId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    private void verifyConsolidate() {
        DataFeed dataFeed = dataFeedProxy.getDataFeed(mainTestTenant.getId());
        Assert.assertEquals(DataFeed.Status.Active, dataFeed.getStatus());

        long numAccounts = countTableRole(BusinessEntity.Account.getBatchStore());
        Assert.assertEquals(numAccounts, 500);
        long numContacts = countTableRole(BusinessEntity.Contact.getBatchStore());
        Assert.assertEquals(numContacts, 500);
    }

}
