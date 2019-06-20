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

import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.cdl.ActivityMetricsProxy;

public class UpdateTransactionDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(UpdateTransactionDeploymentTestNG.class);

    public static final String CHECK_POINT = "update3";

    @Inject
    private ActivityMetricsProxy activityMetricsProxy;

    @Value("${common.test.pls.url}")
    private String deployedHostPort;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint(UpdateContactDeploymentTestNG.CHECK_POINT);
        verifyCheckPoint();

        // To test deprecating curated metrics & action
        setupUpdatedPurchaseHistoryMetrics();

        Assert.assertEquals(Long.valueOf(countInRedshift(BusinessEntity.Account)), ACCOUNT_3);
        Assert.assertEquals(Long.valueOf(countInRedshift(BusinessEntity.Contact)), CONTACT_3);

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
        mockCSVImport(BusinessEntity.Transaction, 3, "DefaultSystem_TransactionData");
        Thread.sleep(2000);
    }

    private void verifyCheckPoint() {
        verifyTxnDailyStore(DAILY_TRANSACTION_DAYS_1, MIN_TRANSACTION_DATE_1, MAX_TRANSACTION_DATE_1, //
                VERIFY_DAILYTXN_AMOUNT_1, //
                VERIFY_DAILYTXN_QUANTITY_1, //
                VERIFY_DAILYTXN_COST);
    }

    private void verifyProcess() {
        runCommonPAVerifications();
        verifyProcessAnalyzeReport(processAnalyzeAppId, getExpectedReport());
        verifyStats(BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.PurchaseHistory, //
                BusinessEntity.CuratedAccount);
        verifyBatchStore(getExpectedBatchStoreCounts());
        verifyRedshift(getExpectedRedshiftCounts());
        verifyServingStore(getExpectedServingStoreCounts());

        verifyTxnDailyStore(DAILY_TRANSACTION_DAYS_2, MIN_TRANSACTION_DATE_2, MAX_TRANSACTION_DATE_2, //
                VERIFY_DAILYTXN_AMOUNT_1 * 2, //
                VERIFY_DAILYTXN_QUANTITY_1 * 2, //
                VERIFY_DAILYTXN_COST * 2);
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

    protected Map<BusinessEntity, Map<String, Object>> getExpectedReport() {
        Map<String, Object> accountReport = new HashMap<>();
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UNMATCH, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        accountReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, ACCOUNT_3);

        Map<String, Object> purchaseHistoryReport = new HashMap<>();
        purchaseHistoryReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL,
                TOTAL_PURCHASE_HISTORY_P2);

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
        transactionReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW,
                TRANSACTION_IN_REPORT_2);
        transactionReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        transactionReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL,
                TRANSACTION_IN_REPORT_3);

        Map<BusinessEntity, Map<String, Object>> expectedReport = new HashMap<>();
        expectedReport.put(BusinessEntity.Account, accountReport);
        expectedReport.put(BusinessEntity.Contact, contactReport);
        expectedReport.put(BusinessEntity.Product, productReport);
        expectedReport.put(BusinessEntity.Transaction, transactionReport);
        expectedReport.put(BusinessEntity.PurchaseHistory, purchaseHistoryReport);

        return expectedReport;
    }

    private Map<BusinessEntity, Long> getExpectedBatchStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_3);
        map.put(BusinessEntity.Contact, CONTACT_3);
        map.put(BusinessEntity.Product, BATCH_STORE_PRODUCT_P2);
        map.put(BusinessEntity.Transaction, TRANSACTION_3);
        map.put(BusinessEntity.PeriodTransaction, PERIOD_TRANSACTION_3);
        return map;
    }

    private Map<BusinessEntity, Long> getExpectedServingStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_3);
        map.put(BusinessEntity.Contact, CONTACT_3);
        map.put(BusinessEntity.Product, SERVING_STORE_PRODUCTS_P2);
        map.put(BusinessEntity.ProductHierarchy, SERVING_STORE_PRODUCT_HIERARCHIES_P2);
        map.put(BusinessEntity.Transaction, TRANSACTION_3);
        map.put(BusinessEntity.PeriodTransaction, PERIOD_TRANSACTION_3);
        return map;
    }

    private Map<BusinessEntity, Long> getExpectedRedshiftCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_3);
        map.put(BusinessEntity.Contact, CONTACT_3);
        return map;
    }

}
