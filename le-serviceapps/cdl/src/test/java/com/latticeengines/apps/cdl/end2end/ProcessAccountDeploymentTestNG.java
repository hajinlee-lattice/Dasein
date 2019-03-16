package com.latticeengines.apps.cdl.end2end;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;

/**
 * Process Account, Contact and Product for a new tenant
 */
public class ProcessAccountDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {
    static final String CHECK_POINT = "process1";
    static final String UNDER_SCORE = "_";

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        try {
            importData();
            processAnalyze();
            verifyProcess();
        } finally {
            if (isLocalEnvironment()) {
                saveCheckpoint(CHECK_POINT);
            }
        }
    }

    // Add sleep between each import to avoid 2 import jobs generate table
    // extract with same timestamp in second, then extract could overwrite
    // between each other
    protected void importData() throws Exception {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        mockCSVImport(BusinessEntity.Account, 1, "Account");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Contact, 1, "Contact");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Product, 1, "ProductBundle");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Product, 2, "ProductHierarchy");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Account, 2, "Account");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Contact, 2, "Contact");
        Thread.sleep(2000);

        // TODO: (Yintao) should be changed to mock vdb import
        mockCSVImport(BusinessEntity.Product, 3, "ProductVDB");
        Thread.sleep(2000);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    protected void verifyProcess() {
        clearCache();
        verifyDataFeedStatus(DataFeed.Status.Active);
        verifyActiveVersion(DataCollection.Version.Green);

        verifyProcessAnalyzeReport(processAnalyzeAppId, getExpectedReport());
        verifyDataCollectionStatus(DataCollection.Version.Green);
        verifyNumAttrsInAccount();
        verifyAccountFeatures();
        verifyDateAttrs();

        // Check that stats cubes only exist for the entities specified below.
        verifyStats(true, BusinessEntity.Account, BusinessEntity.Contact,
                BusinessEntity.CuratedAccount);
        verifyBatchStore(getExpectedbatchStoreCounts());
        verifyRedshift(getExpectedRedshiftCounts());
        verifyServingStore(getExpectedServingStoreCounts());

        createTestSegment3();
        verifySegmentCountsNonNegative(SEGMENT_NAME_3, Arrays.asList(BusinessEntity.Account, BusinessEntity.Contact));
        Map<BusinessEntity, Long> segment3Counts = ImmutableMap.of(//
                BusinessEntity.Account, SEGMENT_3_ACCOUNT_1,
                BusinessEntity.Contact, SEGMENT_3_CONTACT_1);
        verifyTestSegment3Counts(segment3Counts);

        // Create a test segment to verify proper behavior of the Curated Attributes step and resulting table.
        createTestSegmentCuratedAttr();
        verifyTestSegmentCuratedAttrCounts(Collections.singletonMap(BusinessEntity.Account, ACCOUNT_1));
        verifyUpdateActions();
    }

    private void verifyDateAttrs() {
        Table table = dataCollectionProxy.getTable(mainCustomerSpace, BusinessEntity.Account.getBatchStore());
        Assert.assertNotNull(table);
        Attribute attribute = table.getAttribute("user_Test_Date");
        Assert.assertNotNull(attribute);
        Assert.assertTrue(StringUtils.isNotBlank(attribute.getLastDataRefresh()), JsonUtils.serialize(attribute));
    }

    private void verifyNumAttrsInAccount() {
        String tableName = dataCollectionProxy.getTableName(mainCustomerSpace, BusinessEntity.Account.getServingStore());
        List<ColumnMetadata> cms = metadataProxy.getTableColumns(mainCustomerSpace, tableName);
        Assert.assertTrue(cms.size() < 20000, "Should not have more than 20000 account attributes");
    }

    protected Map<BusinessEntity, Map<String, Object>> getExpectedReport() {
        Map<String, Object> accountReport = new HashMap<>();
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.NEW,
                ACCOUNT_1);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.UPDATE, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.UNMATCH, 1L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.DELETE, 0L);
        accountReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + UNDER_SCORE + ReportConstants.TOTAL, ACCOUNT_1);

        Map<String, Object> contactReport = new HashMap<>();
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, CONTACT_1);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, 0L);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        contactReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, CONTACT_1);

        Map<String, Object> productReport = new HashMap<>();
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_ID,
                PRODUCT_ID);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_HIERARCHY,
                PRODUCT_HIERARCHY);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_BUNDLE,
                PRODUCT_BUNDLE);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.WARN_MESSAGE,
                PRODUCT_WARN_MESSAGE);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.ERROR_MESSAGE,
                PRODUCT_ERROR_MESSAGE);

        Map<BusinessEntity, Map<String, Object>> expectedReport = new HashMap<>();
        expectedReport.put(BusinessEntity.Account, accountReport);
        expectedReport.put(BusinessEntity.Contact, contactReport);
        expectedReport.put(BusinessEntity.Product, productReport);

        return expectedReport;
    }

    protected Map<BusinessEntity, Long> getExpectedbatchStoreCounts() {
        return ImmutableMap.of(//
                BusinessEntity.Account, ACCOUNT_1, //
                BusinessEntity.Contact, CONTACT_1, //
                BusinessEntity.Product, BATCH_STORE_PRODUCTS);
    }

    protected Map<BusinessEntity, Long> getExpectedServingStoreCounts() {
        return ImmutableMap.of(//
                BusinessEntity.Product, SERVING_STORE_PRODUCTS, //
                BusinessEntity.ProductHierarchy, SERVING_STORE_PRODUCT_HIERARCHIES);
    }

    protected Map<BusinessEntity, Long> getExpectedRedshiftCounts() {
        return ImmutableMap.of(//
                BusinessEntity.Account, ACCOUNT_1, //
                BusinessEntity.Contact, CONTACT_1);
    }

}
