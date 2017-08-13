package com.latticeengines.apps.cdl.end2end.dataingestion;

import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_2;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_3;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_2;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_3;
import static org.testng.Assert.assertEquals;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class SecondProfileDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SecondProfileDeploymentTestNG.class);

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint("consolidate2");
        verifySecondConsolidateCheckpoint();

        importData();
        profile();
        verifyProfile();

        verifySecondProfileCheckpoint();
    }

    private void importData() throws Exception {
        mockAvroData(BusinessEntity.Account, ACCOUNT_IMPORT_SIZE_1 + ACCOUNT_IMPORT_SIZE_2, ACCOUNT_IMPORT_SIZE_3);
        mockAvroData(BusinessEntity.Contact, CONTACT_IMPORT_SIZE_1 + CONTACT_IMPORT_SIZE_2, CONTACT_IMPORT_SIZE_3);
        Thread.sleep(2000);
    }

    private void profile() {
        log.info("Start profiling ...");
        ApplicationId appId = cdlProxy.profile(mainTestTenant.getId());
        JobStatus completedStatus = waitForWorkflowStatus(appId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
        int numAccounts = ACCOUNT_IMPORT_SIZE_1 + ACCOUNT_IMPORT_SIZE_2 + ACCOUNT_IMPORT_SIZE_3;
        int numContacts = CONTACT_IMPORT_SIZE_1 + CONTACT_IMPORT_SIZE_2 + CONTACT_IMPORT_SIZE_3;
        verifyReport(appId.toString(), 2, numAccounts, numContacts);
    }

    private void verifyProfile() {
        DataFeed dataFeed = dataFeedProxy.getDataFeed(mainTestTenant.getId());
        Assert.assertEquals(DataFeed.Status.Active, dataFeed.getStatus());

        long numAccounts = ACCOUNT_IMPORT_SIZE_1 + ACCOUNT_IMPORT_SIZE_2 + ACCOUNT_IMPORT_SIZE_3;
        long numContacts = CONTACT_IMPORT_SIZE_1 + CONTACT_IMPORT_SIZE_2 + CONTACT_IMPORT_SIZE_3;
        Assert.assertEquals(countTableRole(BusinessEntity.Account.getBatchStore()), numAccounts);
        Assert.assertEquals(countTableRole(BusinessEntity.Contact.getBatchStore()), numContacts);
        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
    }

}
