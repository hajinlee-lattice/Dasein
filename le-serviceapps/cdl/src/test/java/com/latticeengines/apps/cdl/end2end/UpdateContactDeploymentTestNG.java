package com.latticeengines.apps.cdl.end2end;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;

public class UpdateContactDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(UpdateContactDeploymentTestNG.class);

    static final String CHECK_POINT = "update2";

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint(UpdateAccountDeploymentTestNG.CHECK_POINT);
        Assert.assertEquals(Long.valueOf(countInRedshift(BusinessEntity.Contact)), CONTACT_1);

        new Thread(this::createTestSegment3).start();

        importData();
        if (isLocalEnvironment()) {
            processAnalyzeSkipPublishToS3();
        } else {
            processAnalyze();
        }
        try {
            verifyProcess();
        } finally {
            if (isLocalEnvironment()) {
                saveCheckpoint(CHECK_POINT);
            }
        }
    }

    private void importData() throws Exception {
        mockCSVImport(BusinessEntity.Contact, 2, "DefaultSystem_ContactData");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Contact, 3, "DefaultSystem_ContactData");
        Thread.sleep(2000);
    }

    private void verifyProcess() {
        clearCache();
        runCommonPAVerifications();

        verifyProcessAnalyzeReport(processAnalyzeAppId, getExpectedReport());
        verifyStats(BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.PurchaseHistory, //
                BusinessEntity.CuratedAccount);
        verifyBatchStore(getExpectedBatchStoreCounts());
        verifyRedshift(getExpectedRedshiftCounts());
        verifyServingStore(getExpectedServingStoreCounts());

        verifySegmentCountsNonNegative(SEGMENT_NAME_3, Arrays.asList(BusinessEntity.Account, BusinessEntity.Contact));
        Map<BusinessEntity, Long> segment3Counts = ImmutableMap.of( //
                BusinessEntity.Account, SEGMENT_3_ACCOUNT_2,
                BusinessEntity.Contact, SEGMENT_3_CONTACT_2);
        verifyTestSegment3Counts(segment3Counts);
    }

    private Map<BusinessEntity, Long> getExpectedBatchStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_3);
        map.put(BusinessEntity.Contact, CONTACT_3);
        map.put(BusinessEntity.Product, BATCH_STORE_PRODUCT_P2);
        map.put(BusinessEntity.Transaction, TRANSACTION_1);
        map.put(BusinessEntity.PeriodTransaction, PERIOD_TRANSACTION_1);
        return map;
    }

    private Map<BusinessEntity, Long> getExpectedServingStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_3);
        map.put(BusinessEntity.Contact, CONTACT_3);
        map.put(BusinessEntity.Product, SERVING_STORE_PRODUCTS_P2);
        map.put(BusinessEntity.ProductHierarchy, SERVING_STORE_PRODUCT_HIERARCHIES_P2);
        map.put(BusinessEntity.Transaction, TRANSACTION_1);
        map.put(BusinessEntity.PeriodTransaction, PERIOD_TRANSACTION_1);
        return map;
    }

    private Map<BusinessEntity, Long> getExpectedRedshiftCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_3);
        map.put(BusinessEntity.Contact, CONTACT_3);
        return map;
    }

    protected Map<BusinessEntity, Map<String, Object>> getExpectedReport() {
        Map<String, Object> accountReport = new HashMap<>();
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UNMATCH, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        accountReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, ACCOUNT_3);

        Map<String, Object> purchaseHistoryReport = new HashMap<>();
        purchaseHistoryReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, 0L);

        Map<String, Object> contactReport = new HashMap<>();
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, CONTACT_2);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, UPDATED_CONTACT);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        contactReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, CONTACT_3);

        Map<String, Object> productReport = new HashMap<>();
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_ID, 0L);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_HIERARCHY, 0L);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_BUNDLE, 0L);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.ERROR_MESSAGE, "");
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.WARN_MESSAGE, "");

        Map<String, Object> transactionReport = new HashMap<>();
        transactionReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, 0L);
        transactionReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        transactionReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL,
                NEW_TRANSACTION_P2);

        Map<BusinessEntity, Map<String, Object>> expectedReport = new HashMap<>();
        expectedReport.put(BusinessEntity.Account, accountReport);
        expectedReport.put(BusinessEntity.Contact, contactReport);
        expectedReport.put(BusinessEntity.Product, productReport);
        expectedReport.put(BusinessEntity.Transaction, transactionReport);
        expectedReport.put(BusinessEntity.PurchaseHistory, purchaseHistoryReport);

        return expectedReport;
    }

}
