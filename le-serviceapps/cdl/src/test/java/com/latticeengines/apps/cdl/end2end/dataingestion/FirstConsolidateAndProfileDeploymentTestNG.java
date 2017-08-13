package com.latticeengines.apps.cdl.end2end.dataingestion;

import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_1;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;
import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class FirstConsolidateAndProfileDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(FirstConsolidateAndProfileDeploymentTestNG.class);

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        importData();
        consolidate();
        verifyConsolidate();
        profile();
        verifyProfile();

        verifyFirstProfileCheckpoint();
    }

    private void importData() throws Exception {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        mockAvroData(BusinessEntity.Account, 0, ACCOUNT_IMPORT_SIZE_1);
        mockAvroData(BusinessEntity.Contact, 0, CONTACT_IMPORT_SIZE_1);
        Thread.sleep(2000);
        mockAvroData(BusinessEntity.Account, ACCOUNT_IMPORT_SIZE_1, 100);
        mockAvroData(BusinessEntity.Contact, CONTACT_IMPORT_SIZE_1, 100);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    private void consolidate() {
        log.info("Start consolidating ...");
        ApplicationId appId = cdlProxy.consolidate(mainTestTenant.getId());
        JobStatus completedStatus = waitForWorkflowStatus(appId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
        verifyReport(appId.toString(), 0, 0, 0);
    }

    private void verifyConsolidate() {
        DataFeed dataFeed = dataFeedProxy.getDataFeed(mainTestTenant.getId());
        Assert.assertEquals(DataFeed.Status.InitialConsolidated, dataFeed.getStatus());

        long numAccounts = countTableRole(BusinessEntity.Account.getBatchStore());
        Assert.assertEquals(numAccounts, ACCOUNT_IMPORT_SIZE_1);
        long numContacts = countTableRole(BusinessEntity.Contact.getBatchStore());
        Assert.assertEquals(numContacts, CONTACT_IMPORT_SIZE_1);
    }

    private void profile() throws IOException {
        log.info("Start profiling ...");
        ApplicationId appId = cdlProxy.profile(mainTestTenant.getId());
        JobStatus completedStatus = waitForWorkflowStatus(appId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
        verifyReport(appId.toString(), 2, ACCOUNT_IMPORT_SIZE_1, CONTACT_IMPORT_SIZE_1);
    }

    private void verifyProfile() throws IOException {
        DataFeed dataFeed = dataFeedProxy.getDataFeed(mainTestTenant.getId());
        Assert.assertEquals(DataFeed.Status.Active, dataFeed.getStatus());

        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        Table bucketedAccountTable = dataCollectionProxy.getTable(customerSpace,
                BusinessEntity.Account.getServingStore());
        Assert.assertNotNull(bucketedAccountTable);
        List<Attribute> attributes = bucketedAccountTable.getAttributes();
        for (Attribute attribute : attributes) {
            Assert.assertFalse(attribute.getName().contains(CEAttr),
                    "Should not have encoded attr " + attribute.getName() + " in expanded table.");
        }

        Table bucketedContactTable = dataCollectionProxy.getTable(customerSpace,
                BusinessEntity.Contact.getServingStore());
        Assert.assertNotNull(bucketedContactTable);
        attributes = bucketedContactTable.getAttributes();
        for (Attribute attribute : attributes) {
            Assert.assertFalse(attribute.getName().contains(CEAttr),
                    "Should not have encoded attr " + attribute.getName() + " in expanded table.");
        }

        Assert.assertEquals(countInRedshift(BusinessEntity.Account), ACCOUNT_IMPORT_SIZE_1);
    }

}
