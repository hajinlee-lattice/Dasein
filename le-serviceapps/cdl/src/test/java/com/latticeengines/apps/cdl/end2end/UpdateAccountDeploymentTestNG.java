package com.latticeengines.apps.cdl.end2end;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;

public class UpdateAccountDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    public static final String CHECK_POINT = "update1";

    private Date segment3Updated;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint(resumeFromCheckPoint());
        Assert.assertEquals(Long.valueOf(countInRedshift(BusinessEntity.Account)), getPrePAAccountCount());

        new Thread(() -> {
            createTestSegment3();
            MetadataSegment segment3 = getSegmentByName(SEGMENT_NAME_3);
            segment3Updated = segment3.getUpdated();
        }).start();

        createSystems();
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
                saveCheckpoint(saveToCheckPoint());
            }
        }

    }

    /**
     * Create all {@link S3ImportSystem} required by e2e test
     */
    protected void createSystems() {
        // do nothing
    }

    protected Long getPrePAAccountCount() {
        return ACCOUNT_PA;
    }

    protected void importData() throws Exception {
        mockCSVImport(BusinessEntity.Account, 2, "DefaultSystem_AccountData");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Account, 3, "DefaultSystem_AccountData");
        Thread.sleep(2000);
    }

    protected void verifyProcess() {
        clearCache();
        runCommonPAVerifications();
        verifyProcessAnalyzeReport(processAnalyzeAppId, getExpectedReport());
        verifyStats(getEntitiesInStats());
        verifyBatchStore(getExpectedBatchStoreCounts());
        verifyRedshift(getExpectedRedshiftCounts());
        verifyServingStore(getExpectedServingStoreCounts());
        verifyExtraTableRoles(getExtraTableRoeCounts());

        verifySegmentCountsNonNegative(SEGMENT_NAME_3, Arrays.asList(BusinessEntity.Account, BusinessEntity.Contact));
        MetadataSegment segment3 = getSegmentByName(SEGMENT_NAME_3);
        Assert.assertEquals(segment3Updated, segment3.getUpdated());
        Map<BusinessEntity, Long> segment3Counts = getSegmentCounts(SEGMENT_NAME_3);
        verifyTestSegment3Counts(segment3Counts);
    }

    protected Map<BusinessEntity, Long> getSegmentCounts(String segmentName) {
        if (SEGMENT_NAME_3.equals(segmentName)) {
            return ImmutableMap.of( //
                    BusinessEntity.Account, SEGMENT_3_ACCOUNT_1, //
                    BusinessEntity.Contact, SEGMENT_3_CONTACT_1);
        }
        throw new IllegalArgumentException(String.format("Segment %s is not supported", segmentName));
    }

    protected BusinessEntity[] getEntitiesInStats() {
        return new BusinessEntity[] { BusinessEntity.Account, BusinessEntity.Contact, //
                BusinessEntity.PurchaseHistory, BusinessEntity.CuratedAccount };
    }

    protected Map<BusinessEntity, Map<String, Object>> getExpectedReport() {
        Map<String, Object> accountReport = new HashMap<>();
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, NEW_ACCOUNT_UA);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, UPDATED_ACCOUNT_UA);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UNMATCH, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        accountReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, ACCOUNT_UA);

        Map<String, Object> purchaseHistoryReport = new HashMap<>();
        purchaseHistoryReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL,
                TOTAL_PURCHASE_HISTORY_PT);

        Map<String, Object> contactReport = new HashMap<>();
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, 0L);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, 0L);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        contactReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, CONTACT_PA);

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
                NEW_TRANSACTION_PT);

        Map<BusinessEntity, Map<String, Object>> expectedReport = new HashMap<>();
        expectedReport.put(BusinessEntity.Account, accountReport);
        expectedReport.put(BusinessEntity.Contact, contactReport);
        expectedReport.put(BusinessEntity.Product, productReport);
        expectedReport.put(BusinessEntity.Transaction, transactionReport);
        expectedReport.put(BusinessEntity.PurchaseHistory, purchaseHistoryReport);

        return expectedReport;
    }

    protected Map<BusinessEntity, Long> getExpectedBatchStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_UA);
        map.put(BusinessEntity.Contact, CONTACT_PA);
        map.put(BusinessEntity.Product, BATCH_STORE_PRODUCT_PT);
        map.put(BusinessEntity.Transaction, DAILY_TXN_PT);
        map.put(BusinessEntity.PeriodTransaction, PERIOD_TRANSACTION_PT);
        return map;
    }

    protected Map<BusinessEntity, Long> getExpectedServingStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_UA);
        map.put(BusinessEntity.Contact, CONTACT_PA);
        map.put(BusinessEntity.Product, SERVING_STORE_PRODUCTS_PT);
        map.put(BusinessEntity.ProductHierarchy, SERVING_STORE_PRODUCT_HIERARCHIES_PT);
        map.put(BusinessEntity.Transaction, DAILY_TXN_PT);
        map.put(BusinessEntity.PeriodTransaction, PERIOD_TRANSACTION_PT);
        return map;
    }

    protected Map<TableRoleInCollection, Long> getExtraTableRoeCounts() {
        return ImmutableMap.of(//
                TableRoleInCollection.AccountFeatures, ACCOUNT_UA, //
                TableRoleInCollection.AccountExport, ACCOUNT_UA //
        );
    }

    protected Map<BusinessEntity, Long> getExpectedRedshiftCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_UA);
        map.put(BusinessEntity.Contact, CONTACT_PA);
        return map;
    }

    protected String resumeFromCheckPoint() {
        return ProcessTransactionDeploymentTestNG.CHECK_POINT;
    }

    protected String saveToCheckPoint() {
        return CHECK_POINT;
    }

}
