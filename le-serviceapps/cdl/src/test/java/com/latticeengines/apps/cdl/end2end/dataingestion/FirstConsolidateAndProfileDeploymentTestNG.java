package com.latticeengines.apps.cdl.end2end.dataingestion;

import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_1;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class FirstConsolidateAndProfileDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(FirstConsolidateAndProfileDeploymentTestNG.class);

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        importData();
        verifyCannotProfile();
        consolidate();
        verifyConsolidate();
        profile();
        verifyProfile();

        verifyFirstProfileCheckpoint();
        saveCheckpoint("profile1");
    }

    private void importData() throws Exception {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        mockVdbImport(BusinessEntity.Account, 0, ACCOUNT_IMPORT_SIZE_1);
        mockVdbImport(BusinessEntity.Contact, 0, CONTACT_IMPORT_SIZE_1);
        Thread.sleep(2000);
        mockVdbImport(BusinessEntity.Account, ACCOUNT_IMPORT_SIZE_1, 100);
        mockVdbImport(BusinessEntity.Contact, CONTACT_IMPORT_SIZE_1, 100);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    private void verifyCannotProfile() {
        try {
            profile();
        } catch (Exception e) {
            log.info("Caught an exception as expected: " + e.getMessage());
            return;
        }
        Assert.fail("Should have thrown an exception.");
    }

    private void verifyConsolidate() {
        verifyConsolidateReport(consolidateAppId, 0, 0, 0);
        verifyDataFeedStatsu(DataFeed.Status.InitialConsolidated);

        long numAccounts = countTableRole(BusinessEntity.Account.getBatchStore());
        Assert.assertEquals(numAccounts, ACCOUNT_IMPORT_SIZE_1);
        long numContacts = countTableRole(BusinessEntity.Contact.getBatchStore());
        Assert.assertEquals(numContacts, CONTACT_IMPORT_SIZE_1);

        verifyActiveVersion(DataCollection.Version.Blue);
    }

    private void verifyProfile() throws IOException {
        verifyProfileReport(profileAppId, 2, ACCOUNT_IMPORT_SIZE_1, CONTACT_IMPORT_SIZE_1);
        verifyDataFeedStatsu(DataFeed.Status.Active);
        verifyActiveVersion(DataCollection.Version.Green);

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
