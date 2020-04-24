package com.latticeengines.apps.cdl.end2end;

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;

public class ProcessInvalidProductsDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {
//    private static long FAILED_PRODUCT_IMPORT_COUNT = 0L;
    private static long INITIAL_ACCOUNT_COUNT = 0L;

    private DataCollection.Version initialVersion;

    @Test(groups = "deployment", priority = 0)
    public void testBundleProductMissingProductBundle() throws Exception {
        initialVersion = dataCollectionProxy.getActiveVersion(mainCustomerSpace);
        importData(4, "ProductBundle_MissingProductBundle");
        processAnalyze();
        verifyProcess(8);
    }

    @Test(groups = "deployment", priority = 1)
    public void testHierarchyProductMissingCategory() throws Exception {
        initialVersion = dataCollectionProxy.getActiveVersion(mainCustomerSpace);
        importData(5, "ProductHierarchies_MissingCategory");
        processAnalyze();
        verifyProcess(19);
    }

    @Test(groups = "deployment", priority = 2)
    public void testHierarchyProductMissingFamily() throws Exception {
        initialVersion = dataCollectionProxy.getActiveVersion(mainCustomerSpace);
        importData(6, "ProductHierarchies_MissingFamily");
        processAnalyze();
        verifyProcess(48);
    }

    private void importData(int fileIndex, String datafeedType) throws Exception {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        mockCSVImport(BusinessEntity.Product, fileIndex, datafeedType);
        Thread.sleep(2000);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    void verifyDataCollectionStatus(DataCollection.Version version) {
        DataCollectionStatus dataCollectionStatus = dataCollectionProxy
                .getOrCreateDataCollectionStatus(mainTestTenant.getId(), version);
        Assert.assertEquals(dataCollectionStatus.getAccountCount(), Long.valueOf(INITIAL_ACCOUNT_COUNT));
    }

    private void verifyProcess(long totalProducts) {
        DataCollection.Version version = initialVersion.complement();
        clearCache();
        verifyDataFeedStatus(DataFeed.Status.Active);
        verifyActiveVersion(version);

        Map<String, Object> productReport = new HashMap<>();
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_ID, PRODUCT_ID_PA);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_HIERARCHY, PRODUCT_HIERARCHY_PA);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_BUNDLE, PRODUCT_BUNDLE_PA);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.WARN_MESSAGE, PRODUCT_WARN_MESSAGE);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.ERROR_MESSAGE, PRODUCT_ERROR_MESSAGE);

        Map<BusinessEntity, Map<String, Object>> expectedReport = new HashMap<>();
        expectedReport.put(BusinessEntity.Product, productReport);
        verifyProcessAnalyzeReport(processAnalyzeAppId, expectedReport);
        verifyDataCollectionStatus(version);

        Map<BusinessEntity, Long> batchStoreCounts = ImmutableMap.of(
                BusinessEntity.Product, totalProducts);
        verifyBatchStore(batchStoreCounts);
    }
}
