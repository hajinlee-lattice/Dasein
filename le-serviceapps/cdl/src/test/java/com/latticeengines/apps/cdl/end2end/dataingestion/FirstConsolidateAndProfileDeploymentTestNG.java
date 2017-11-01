package com.latticeengines.apps.cdl.end2end.dataingestion;

import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.PRODUCT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.TRANSACTION_IMPORT_SIZE_1;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class FirstConsolidateAndProfileDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(FirstConsolidateAndProfileDeploymentTestNG.class);

    private static final int ACCOUNT_IMPORT_SIZE_1_1 = 350;
    private static final int ACCOUNT_IMPORT_SIZE_1_2 = 150;
    private static final int CONTACT_IMPORT_SIZE_1_1 = 600;
    private static final int CONTACT_IMPORT_SIZE_1_2 = 500;
    private static final int PRODUCT_IMPORT_SIZE_1_1 = 70;
    private static final int PRODUCT_IMPORT_SIZE_1_2 = 30;
    private static final int TRANSACTION_IMPORT_SIZE_1_1 = 20000;
    private static final int TRANSACTION_IMPORT_SIZE_1_2 = 10000;


    @Test(groups = "end2end")
    public void runTest() throws Exception {
        runPreCheckInTest();
        verifyFirstProfileCheckpoint();
        saveCheckpoint("profile1");
    }

    @Test(groups = "precheckin")
    public void runPreCheckInTest() throws Exception {

        Assert.assertEquals(ACCOUNT_IMPORT_SIZE_1_1 + ACCOUNT_IMPORT_SIZE_1_2, ACCOUNT_IMPORT_SIZE_1);
        Assert.assertEquals(CONTACT_IMPORT_SIZE_1_1 + CONTACT_IMPORT_SIZE_1_2, CONTACT_IMPORT_SIZE_1);
        Assert.assertEquals(PRODUCT_IMPORT_SIZE_1_1 + PRODUCT_IMPORT_SIZE_1_2, PRODUCT_IMPORT_SIZE_1);
        Assert.assertEquals(TRANSACTION_IMPORT_SIZE_1_1 + TRANSACTION_IMPORT_SIZE_1_2, TRANSACTION_IMPORT_SIZE_1);

        importData();
        verifyCannotProfile();
        consolidate();
        verifyConsolidate();
        profile();
        verifyProfile();
    }

    private void importData() throws Exception {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        mockVdbImport(BusinessEntity.Account, 0, ACCOUNT_IMPORT_SIZE_1_1);
        mockVdbImport(BusinessEntity.Contact, 0, CONTACT_IMPORT_SIZE_1_1);
        mockVdbImport(BusinessEntity.Product, 0, PRODUCT_IMPORT_SIZE_1_1);
        mockVdbImport(BusinessEntity.Transaction, 0, TRANSACTION_IMPORT_SIZE_1_1);
        Thread.sleep(2000);
        mockVdbImport(BusinessEntity.Account, ACCOUNT_IMPORT_SIZE_1_1, ACCOUNT_IMPORT_SIZE_1_2);
        mockVdbImport(BusinessEntity.Contact, CONTACT_IMPORT_SIZE_1_1, CONTACT_IMPORT_SIZE_1_2);
        mockVdbImport(BusinessEntity.Product, PRODUCT_IMPORT_SIZE_1_1, PRODUCT_IMPORT_SIZE_1_2);
        mockVdbImport(BusinessEntity.Transaction, TRANSACTION_IMPORT_SIZE_1_1, TRANSACTION_IMPORT_SIZE_1_2);
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
        Map<TableRoleInCollection, Long> expectedCounts = ImmutableMap.of(
                BusinessEntity.Product.getServingStore(), (long) PRODUCT_IMPORT_SIZE_1,
                BusinessEntity.Transaction.getServingStore(), (long) TRANSACTION_IMPORT_SIZE_1);
        verifyConsolidateReport(consolidateAppId, expectedCounts);
        verifyDataFeedStatus(DataFeed.Status.InitialConsolidated);

        long numAccounts = countTableRole(BusinessEntity.Account.getBatchStore());
        Assert.assertEquals(numAccounts, ACCOUNT_IMPORT_SIZE_1);
        long numContacts = countTableRole(BusinessEntity.Contact.getBatchStore());
        Assert.assertEquals(numContacts, CONTACT_IMPORT_SIZE_1);
        long numProducts = countTableRole(BusinessEntity.Product.getBatchStore());
        Assert.assertEquals(numProducts, PRODUCT_IMPORT_SIZE_1);
        long numTransactions = countTableRole(BusinessEntity.Transaction.getServingStore());
        Assert.assertEquals(numTransactions, TRANSACTION_IMPORT_SIZE_1);

        verifyActiveVersion(DataCollection.Version.Blue);
    }

    private void verifyProfile() throws IOException {
        Map<TableRoleInCollection, Long> expectedCounts = ImmutableMap.of( //
                BusinessEntity.Account.getServingStore(), (long) ACCOUNT_IMPORT_SIZE_1,
                BusinessEntity.Contact.getServingStore(), (long) CONTACT_IMPORT_SIZE_1,
                BusinessEntity.Product.getServingStore(), (long) PRODUCT_IMPORT_SIZE_1);
        verifyProfileReport(profileAppId, expectedCounts);
        verifyDataFeedStatus(DataFeed.Status.Active);
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
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), CONTACT_IMPORT_SIZE_1);
    }

}
