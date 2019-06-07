package com.latticeengines.apps.cdl.end2end;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

/**
 * Process Transaction imports after ProcessAccountDeploymentTestNG
 */
public class ProcessTransactionDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ProcessTransactionDeploymentTestNG.class);

    public static final String CHECK_POINT = "process2";

    @Inject
    private EntityProxy entityProxy;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint(resumeFromCheckPoint());
        verifyNumAttrsInAccount();
        verifyDateTypeAttrs();
        new Thread(() -> {
            createTestSegment1();
            createTestSegment2();
        }).start();

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
        mockCSVImport(BusinessEntity.Transaction, 1, "DefaultSystem_TransactionData");
        Thread.sleep(1100);
        mockCSVImport(BusinessEntity.Transaction, 2, "DefaultSystem_TransactionData");
        Thread.sleep(2000);
    }

    protected String resumeFromCheckPoint() {
        return ProcessAccountDeploymentTestNG.CHECK_POINT;
    }

    protected String saveToCheckPoint() {
        return CHECK_POINT;
    }

    protected void verifyProcess() {
        clearCache();
        runCommonPAVerifications();

        verifyNumAttrsInAccount();
        verifyProcessAnalyzeReport(processAnalyzeAppId, getExpectedReport());
        verifyStats(BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.PurchaseHistory, //
                BusinessEntity.CuratedAccount);
        verifyBatchStore(getExpectedBatchStoreCounts());
        // verifyServingStore(getExpectedServingStoreCounts());
        // verifyRedshift(getExpectedRedshiftCounts());
        verifyTxnDailyStore(DAILY_TRANSACTION_DAYS_1, MIN_TRANSACTION_DATE_1, MAX_TRANSACTION_DATE_1, //
                VERIFY_DAILYTXN_AMOUNT_1, //
                VERIFY_DAILYTXN_QUANTITY_1, //
                VERIFY_DAILYTXN_COST);
    }

    private void verifyNumAttrsInAccount() {
        String tableName = dataCollectionProxy.getTableName(mainCustomerSpace,
                BusinessEntity.Account.getServingStore());
        List<ColumnMetadata> cms = metadataProxy.getTableColumns(mainCustomerSpace, tableName);
        Assert.assertTrue(cms.size() < 20000, "Should not have more than 20000 account attributes");
    }

    protected void verifyDateTypeAttrs() {
        FrontEndQuery query = new FrontEndQuery();
        query.setMainEntity(BusinessEntity.Account);
        Bucket bkt = Bucket.dateBkt(TimeFilter.ever());
        Restriction restriction = new BucketRestriction(BusinessEntity.Account, "user_Test_Date", bkt);
        query.setAccountRestriction(new FrontEndRestriction(restriction));
        Long count = entityProxy.getCount(mainCustomerSpace, query);
        Assert.assertEquals(count, ACCOUNT_1);

        bkt = Bucket.dateBkt(TimeFilter.isEmpty());
        restriction = new BucketRestriction(BusinessEntity.Account, "user_Test_Date", bkt);
        query.setAccountRestriction(new FrontEndRestriction(restriction));
        count = entityProxy.getCount(mainCustomerSpace, query);
        Assert.assertEquals(count.longValue(), expectedUserTestDateCntsBeforePA());

        bkt = Bucket.dateBkt(TimeFilter.latestDay());
        restriction = new BucketRestriction(BusinessEntity.Account, "user_Test_Date", bkt);
        query.setAccountRestriction(new FrontEndRestriction(restriction));
        count = entityProxy.getCount(mainCustomerSpace, query);
        log.info("count " + count);
        Assert.assertTrue(count > 0);
        Assert.assertTrue(count < ACCOUNT_1);
        log.info("verify date done");
    }

    protected int expectedUserTestDateCntsBeforePA() {
        return 0;
    }

    protected Map<BusinessEntity, Map<String, Object>> getExpectedReport() {
        Map<String, Object> transactionReport = new HashMap<>();
        transactionReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW,
                TRANSACTION_IN_REPORT_1);
        transactionReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        transactionReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL,
                TRANSACTION_IN_REPORT_1);

        Map<String, Object> purchaseHistoryReport = new HashMap<>();
        purchaseHistoryReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL,
                PURCHASE_HISTORY_1);

        Map<BusinessEntity, Map<String, Object>> expectedReport = new HashMap<>();
        expectedReport.put(BusinessEntity.Transaction, transactionReport);
        expectedReport.put(BusinessEntity.PurchaseHistory, purchaseHistoryReport);
        return expectedReport;
    }

    protected Map<BusinessEntity, Long> getExpectedBatchStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_1);
        map.put(BusinessEntity.Contact, CONTACT_1);
        map.put(BusinessEntity.Product, BATCH_STORE_PRODUCTS);
        map.put(BusinessEntity.Transaction, TRANSACTION_1);
        map.put(BusinessEntity.PeriodTransaction, PERIOD_TRANSACTION_1);
        return map;
    }

    protected Map<BusinessEntity, Long> getExpectedServingStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_1);
        map.put(BusinessEntity.Contact, CONTACT_1);
        map.put(BusinessEntity.Product, SERVING_STORE_PRODUCTS);
        map.put(BusinessEntity.ProductHierarchy, SERVING_STORE_PRODUCT_HIERARCHIES);
        map.put(BusinessEntity.Transaction, TRANSACTION_1);
        map.put(BusinessEntity.PeriodTransaction, PERIOD_TRANSACTION_1);
        return map;
    }

    protected Map<BusinessEntity, Long> getExpectedRedshiftCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_1);
        map.put(BusinessEntity.Contact, CONTACT_1);
        return map;
    }

}
