package com.latticeengines.apps.cdl.end2end.dataingestion;

import java.util.Arrays;
import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ProcessAccountWithImportDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {
    static final String CHECK_POINT = "process1";

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        try {
            runPreCheckin();
        } finally {
            //saveCheckpoint(CHECK_POINT);
        }
    }

    @Test(groups = "precheckin")
    public void runPreCheckin() throws Exception {
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
        importData();
        processAnalyze();
        verifyProcess();
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
        mockCSVImport(BusinessEntity.Product, 3, "ProductBundle");
        Thread.sleep(2000);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    private void verifyProcess() {
        verifyDataFeedStatus(DataFeed.Status.Active);
        verifyActiveVersion(DataCollection.Version.Green);

        verifyProcessAnalyzeReport(processAnalyzeAppId);

        StatisticsContainer statisticsContainer = dataCollectionProxy.getStats(mainTestTenant.getId());
        Assert.assertNotNull(statisticsContainer, "Should have statistics in active version");

        verifyStats(BusinessEntity.Account, BusinessEntity.Contact);

        long numAccounts = 500;
        long numContacts = 500;
        long numProducts = 100;

        Assert.assertEquals(countTableRole(BusinessEntity.Account.getBatchStore()), numAccounts);
        Assert.assertEquals(countTableRole(BusinessEntity.Contact.getBatchStore()), numContacts);
        Assert.assertEquals(countTableRole(BusinessEntity.Product.getBatchStore()), numProducts);

        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), numContacts);

        createTestSegment2();
        verifySegmentCountsNonNegative(SEGMENT_NAME_2, Arrays.asList(BusinessEntity.Account, BusinessEntity.Contact));
        verifyUpdateActions();
    }
}
