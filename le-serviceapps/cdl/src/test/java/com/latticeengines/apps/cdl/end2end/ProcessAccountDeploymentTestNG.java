package com.latticeengines.apps.cdl.end2end;

import java.util.List;

import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;


/**
 * Process Account, Contact and Product for a new tenant
 */
public class ProcessAccountDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {
    static final String CHECK_POINT = "process1";

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        try {
            importData();
            processAnalyze();
            verifyProcess();
        } finally {
            saveCheckpoint(CHECK_POINT);
        }
    }

    private void importData() throws Exception {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        mockCSVImport(BusinessEntity.Account, 1, "Account");
        mockCSVImport(BusinessEntity.Contact, 1, "Contact");
        mockCSVImport(BusinessEntity.Product, 1, "ProductBundle");
        mockCSVImport(BusinessEntity.Product, 2, "ProductHierarchy");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Account, 2, "Account");
        mockCSVImport(BusinessEntity.Contact, 2, "Contact");
        // TODO: (Yintao) should be changed to mock vdb import
        mockCSVImport(BusinessEntity.Product, 3, "ProductVDB");
        Thread.sleep(2000);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    private void verifyProcess() {
        verifyDataFeedStatus(DataFeed.Status.Active);
        verifyActiveVersion(DataCollection.Version.Green);

        verifyProcessAnalyzeReport(processAnalyzeAppId);
        verifyDataCollectionStatus(DataCollection.Version.Green);
        verifyNumAttrsInAccount();

        StatisticsContainer statisticsContainer = dataCollectionProxy.getStats(mainTestTenant.getId());
        Assert.assertNotNull(statisticsContainer, "Should have statistics in active version");

        verifyStats(true, BusinessEntity.Account, BusinessEntity.Contact);

//        long numAccounts = 500;
//        long numContacts = 500;
//
//        Assert.assertEquals(countTableRole(BusinessEntity.Account.getBatchStore()), numAccounts);
//        Assert.assertEquals(countTableRole(BusinessEntity.Contact.getBatchStore()), numContacts);
//        Assert.assertEquals(countTableRole(BusinessEntity.Product.getBatchStore()), 99);
//
//        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
//        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), 99);
//
        createTestSegment2();
        verifySegmentCountsNonNegative(SEGMENT_NAME_2, Arrays.asList(BusinessEntity.Account, BusinessEntity.Contact));

//        verifyUpdateActions();
    }


    private void verifyNumAttrsInAccount() {
        String tableName = dataCollectionProxy.getTableName(mainCustomerSpace, BusinessEntity.Account.getServingStore());
        List<ColumnMetadata> cms = metadataProxy.getTableColumns(mainCustomerSpace, tableName);
        Assert.assertTrue(cms.size() < 20000, "Should not have more than 20000 account attributes");
    }

}
