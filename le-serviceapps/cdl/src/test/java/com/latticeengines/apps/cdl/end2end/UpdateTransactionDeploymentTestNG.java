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

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(UpdateTransactionDeploymentTestNG.class);

    public static final String CHECK_POINT = "update3";

    @Inject
    private ActivityMetricsProxy activityMetricsProxy;

    @Value("${common.test.pls.url}")
    private String deployedHostPort;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint(resumeFromCheckPoint());
        verifyCheckPoint();

        // To test deprecating curated metrics & action
        setupUpdatedPurchaseHistoryMetrics();

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

    protected void importData() throws Exception {
        mockCSVImport(BusinessEntity.Transaction, 3, "DefaultSystem_TransactionData");
        Thread.sleep(2000);
    }

    protected String resumeFromCheckPoint() {
        return UpdateContactDeploymentTestNG.CHECK_POINT;
    }

    protected String saveToCheckPoint() {
        return CHECK_POINT;
    }

    protected void verifyCheckPoint() {
        Assert.assertEquals(Long.valueOf(countInRedshift(BusinessEntity.Account)), ACCOUNT_UA);
        Assert.assertEquals(Long.valueOf(countInRedshift(BusinessEntity.Contact)), CONTACT_UC);
        verifyTxnDailyStore(DAILY_TXN_DAYS_PT, MIN_TXN_DATE_PT, MAX_TXN_DATE_PT, //
                VERIFY_DAILYTXN_AMOUNT_PT, //
                VERIFY_DAILYTXN_QUANTITY_PT, //
                VERIFY_DAILYTXN_COST_PT);
    }

    private void verifyProcess() {
        runCommonPAVerifications();
        verifyProcessAnalyzeReport(processAnalyzeAppId, getExpectedReport());
        verifyStats(BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.PurchaseHistory, //
                BusinessEntity.CuratedAccount);
        verifyBatchStore(getExpectedBatchStoreCounts());
        verifyRedshift(getExpectedRedshiftCounts());
        verifyServingStore(getExpectedServingStoreCounts());
        verifyTxnDailyStore();
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
        accountReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, ACCOUNT_UA);

        Map<String, Object> contactReport = new HashMap<>();
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, 0L);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, 0L);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        contactReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, CONTACT_UC);

        Map<String, Object> productReport = new HashMap<>();
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_ID, 0L);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_HIERARCHY, 0L);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_BUNDLE, 0L);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.ERROR_MESSAGE, "");
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.WARN_MESSAGE, "");

        Map<String, Object> transactionReport = new HashMap<>();
        transactionReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW,
                NEW_TRANSACTION_UT);
        transactionReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        transactionReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL,
                TOTAL_TRANSACTION_UT);

        Map<String, Object> purchaseHistoryReport = new HashMap<>();
        purchaseHistoryReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL,
                TOTAL_PURCHASE_HISTORY_PT);

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
        map.put(BusinessEntity.Contact, CONTACT_UC);
        map.put(BusinessEntity.Product, BATCH_STORE_PRODUCT_PT);
        map.put(BusinessEntity.Transaction, DAILY_TXN_UT);
        map.put(BusinessEntity.PeriodTransaction, PERIOD_TRANSACTION_UT);
        return map;
    }

    protected Map<BusinessEntity, Long> getExpectedServingStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_UA);
        map.put(BusinessEntity.Contact, CONTACT_UC);
        map.put(BusinessEntity.Product, SERVING_STORE_PRODUCTS_PT);
        map.put(BusinessEntity.ProductHierarchy, SERVING_STORE_PRODUCT_HIERARCHIES_PT);
        map.put(BusinessEntity.Transaction, DAILY_TXN_UT);
        map.put(BusinessEntity.PeriodTransaction, PERIOD_TRANSACTION_UT);
        return map;
    }

    protected Map<BusinessEntity, Long> getExpectedRedshiftCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_UA);
        map.put(BusinessEntity.Contact, CONTACT_UC);
        return map;
    }

    protected void verifyTxnDailyStore() {
        verifyTxnDailyStore(DAILY_TXN_DAYS_UT, MIN_TXN_DATE_UT, MAX_TXN_DATE_UT, //
                VERIFY_DAILYTXN_AMOUNT_PT * 2, //
                VERIFY_DAILYTXN_QUANTITY_PT * 2, //
                VERIFY_DAILYTXN_COST_PT * 2);
    }

}
