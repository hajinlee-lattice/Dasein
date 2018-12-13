package com.latticeengines.apps.cdl.end2end;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.cdl.ActivityMetricsProxy;

public class UpdateTransactionDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(UpdateTransactionDeploymentTestNG.class);

    static final String CHECK_POINT = "update3";

    @Inject
    private ActivityMetricsProxy activityMetricsProxy;

    @Value("${common.test.pls.url}")
    private String deployedHostPort;

    private RatingEngine ratingEngine;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint(UpdateContactDeploymentTestNG.CHECK_POINT);
        verifyCheckPoint();

        // To test deprecating curated metrics & action
        setupUpdatedPurchaseHistoryMetrics();

        Assert.assertEquals(countInRedshift(BusinessEntity.Account), 1000);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), 1000);

        importData();
        processAnalyze();
        try {
            verifyProcess();
        } finally {
            if (isLocalEnvironment()) {
                saveCheckpoint(CHECK_POINT);
            }
        }
    }

    private void importData() throws Exception {
        mockCSVImport(BusinessEntity.Transaction, 3, "Transaction");
        Thread.sleep(2000);
    }

    private void verifyCheckPoint() throws Exception {
        verifyTxnDailyStore(DAILY_TRANSACTION_DAYS1, //
                DateTimeUtils.dateToDayPeriod(MIN_TRANSACTION_DATE1), //
                DateTimeUtils.dateToDayPeriod(MAX_TRANSACTION_DATE1), //
                VERIFY_DAILYTXN_AMOUNT1, VERIFY_DAILYTXN_QUANTITY1, VERIFY_DAILYTXN_COST);
    }

    private void verifyProcess() throws Exception {
        runCommonPAVerifications();

        Map<String, Object> accountReport = new HashMap<>();
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UNMATCH, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        accountReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, ACCOUNT_3);

        Map<String, Object> purchaseHistoryReport = new HashMap<>();
        purchaseHistoryReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, 5L);

        Map<String, Object> contactReport = new HashMap<>();
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, 0L);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, 0L);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        contactReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, CONTACT_3);

        Map<String, Object> productReport = new HashMap<>();
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_ID, 0L);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_HIERARCHY, 0L);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_BUNDLE, 0L);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.ERROR_MESSAGE, "");
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.WARN_MESSAGE, "");

        Map<String, Object> transactionReport = new HashMap<>();
        transactionReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, TRANSACTION_2);
        transactionReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        transactionReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, TRANSACTION_3);

        Map<BusinessEntity, Map<String, Object>> expectedReport = new HashMap<>();
        expectedReport.put(BusinessEntity.Account, accountReport);
        expectedReport.put(BusinessEntity.Contact, contactReport);
        expectedReport.put(BusinessEntity.Product, productReport);
        expectedReport.put(BusinessEntity.Transaction, transactionReport);
        expectedReport.put(BusinessEntity.PurchaseHistory, purchaseHistoryReport);

        verifyProcessAnalyzeReport(processAnalyzeAppId, expectedReport);

//        long numAccounts = ACCOUNT_IMPORT_SIZE_TOTAL;
//        long numContacts = CONTACT_IMPORT_SIZE_TOTAL;
//        long numTransactions = TRANSACTION_IMPORT_SIZE_1 + TRANSACTION_IMPORT_SIZE_2;
//
//        Assert.assertEquals(countTableRole(TableRoleInCollection.ConsolidatedRawTransaction), numTransactions);
//

        Map<BusinessEntity, Long> batchStoreCounts = ImmutableMap.of( //
                BusinessEntity.Product, BATCH_STORE_PRODUCTS);
        verifyBatchStore(batchStoreCounts);

        Map<BusinessEntity, Long> servingStoreCounts = ImmutableMap.of( //
                BusinessEntity.Product, SERVING_STORE_PRODUCTS, //
                BusinessEntity.ProductHierarchy, SERVING_STORE_PRODUCT_HIERARCHIES);
        verifyServingStore(servingStoreCounts);

//        verityTestSegmentCountDiff(ImmutableList.of(BusinessEntity.Account, BusinessEntity.Contact));
        /*
        Map<BusinessEntity, Long> segment1Counts = ImmutableMap.of( //
                BusinessEntity.Account, SEGMENT_1_ACCOUNT_4,
                BusinessEntity.Contact, SEGMENT_1_CONTACT_4);
        verifyTestSegment1Counts(segment1Counts);
        Map<BusinessEntity, Long> segment2Counts = ImmutableMap.of( //
                BusinessEntity.Account, SEGMENT_2_ACCOUNT_2_REBUILD, BusinessEntity.Contact,
                SEGMENT_2_CONTACT_2_REBUILD);
        verifyTestSegment2Counts(segment2Counts);
        */
//        Map<RatingBucketName, Long> ratingCounts = ImmutableMap.of( //
//                RatingBucketName.A, RATING_A_COUNT_2_REBUILD, //
//                RatingBucketName.D, RATING_D_COUNT_2_REBUILD, //
//                RatingBucketName.F, RATING_F_COUNT_2_REBUILD
//        );
        // TODO: use rating proxy
        // verifyRatingEngineCount(ratingEngine.getId(), ratingCounts);

        verifyTxnDailyStore(DAILY_TRANSACTION_DAYS2, //
                DateTimeUtils.dateToDayPeriod(MIN_TRANSACTION_DATE2), //
                DateTimeUtils.dateToDayPeriod(MAX_TRANSACTION_DATE2), //
                VERIFY_DAILYTXN_AMOUNT1 * 2, VERIFY_DAILYTXN_QUANTITY1 * 2, VERIFY_DAILYTXN_COST * 2);
    }

    private void setupUpdatedPurchaseHistoryMetrics() {
        // Deprecated Total/Avg SP metrics with WITHIN relation and create new
        // ones with BETWEEN relation
        List<ActivityMetrics> metrics = activityMetricsProxy.getActiveActivityMetrics(mainCustomerSpace,
                ActivityType.PurchaseHistory);
        RestTemplate restTemplate = testBed.getRestTemplate();
        deployedHostPort = deployedHostPort.endsWith("/") ? deployedHostPort.substring(0, deployedHostPort.length() - 1)
                : deployedHostPort;
        restTemplate.postForObject(deployedHostPort + "/pls/datacollection/metrics/PurchaseHistory", metrics,
                List.class);
    }


}
