package com.latticeengines.apps.cdl.end2end.dataingestion;

import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_2;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_2;
import static org.testng.Assert.assertEquals;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class SecondConsolidateDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SecondConsolidateDeploymentTestNG.class);

    private StatisticsContainer preConsolidateStats;

    @Test(groups = "end2end")
    public void testFirstConsolidate() throws Exception {
        resumeCheckpoint("profile1");
        verifyFirstProfileCheckpoint();
        log.info("Exporting checkpoint data to redshift. This may take more than 20 min ...");
        exportEntityToRedshift(BusinessEntity.Account);
        exportEntityToRedshift(BusinessEntity.Contact);
        Assert.assertEquals(countInRedshift(BusinessEntity.Account), ACCOUNT_IMPORT_SIZE_1);
        // Assert.assertEquals(countInRedshift(BusinessEntity.Contact),
        // CONTACT_IMPORT_SIZE_1);

        preConsolidateStats = dataCollectionProxy.getStats(mainTestTenant.getId());

        importData();
        consolidate();
        verifyConsolidate();

        verifySecondConsolidateCheckpoint();
    }

    private void importData() throws Exception {
        mockAvroData(BusinessEntity.Account, ACCOUNT_IMPORT_SIZE_1, ACCOUNT_IMPORT_SIZE_2);
        mockAvroData(BusinessEntity.Contact, CONTACT_IMPORT_SIZE_1, CONTACT_IMPORT_SIZE_2);
        Thread.sleep(2000);
    }

    private void consolidate() {
        log.info("Start consolidating ...");
        ApplicationId appId = cdlProxy.consolidate(mainTestTenant.getId());
        JobStatus completedStatus = waitForWorkflowStatus(appId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
        verifyReport(appId.toString(), 2, ACCOUNT_IMPORT_SIZE_2);
    }

    private void verifyConsolidate() {
        DataFeed dataFeed = dataFeedProxy.getDataFeed(mainTestTenant.getId());
        Assert.assertEquals(DataFeed.Status.Active, dataFeed.getStatus());

        long numAccounts = ACCOUNT_IMPORT_SIZE_1 + ACCOUNT_IMPORT_SIZE_2;
        long numContacts = CONTACT_IMPORT_SIZE_1 + CONTACT_IMPORT_SIZE_2;
        Assert.assertEquals(countTableRole(BusinessEntity.Account.getBatchStore()), numAccounts);
        Assert.assertEquals(countTableRole(BusinessEntity.Contact.getBatchStore()), numContacts);
        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
        // Assert.assertEquals(countInRedshift(BusinessEntity.Contact),
        // numContacts);

        StatisticsContainer postConsolidateStats = dataCollectionProxy.getStats(mainTestTenant.getId());
        Assert.assertEquals(preConsolidateStats.getName(), postConsolidateStats.getName());
    }

}
