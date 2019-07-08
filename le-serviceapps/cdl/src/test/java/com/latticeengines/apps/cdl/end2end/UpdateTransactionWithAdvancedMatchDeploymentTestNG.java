package com.latticeengines.apps.cdl.end2end;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;

public class UpdateTransactionWithAdvancedMatchDeploymentTestNG extends UpdateTransactionDeploymentTestNG {
    private static final Logger log = LoggerFactory.getLogger(UpdateTransactionWithAdvancedMatchDeploymentTestNG.class);

    static final String CHECK_POINT = "entitymatch_update2";

    @BeforeClass(groups = "end2end")
    @Override
    public void setup() throws Exception {
        log.info("Running setup with ENABLE_ENTITY_MATCH enabled!");
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName(), true);
        setupEnd2EndTestEnvironment(featureFlagMap);
        checkpointService
                .setPrecedingCheckpoints(Arrays.asList( //
                        ProcessAccountWithAdvancedMatchDeploymentTestNG.CHECK_POINT, //
                        ProcessTransactionWithAdvancedMatchDeploymentTestNG.CHECK_POINT //
                ));
        log.info("Setup Complete!");
    }

    @Override
    protected void importData() throws Exception {
        mockCSVImport(BusinessEntity.Transaction, ADVANCED_MATCH_SUFFIX, 3, "DefaultSystem_TransactionData");
        Thread.sleep(2000);
    }

    @Override
    protected String resumeFromCheckPoint() {
        return ProcessTransactionWithAdvancedMatchDeploymentTestNG.CHECK_POINT;
    }

    @Override
    protected String saveToCheckPoint() {
        return CHECK_POINT;
    }

    @Override
    protected void verifyCheckPoint() {
        Assert.assertEquals(Long.valueOf(countInRedshift(BusinessEntity.Account)), ACCOUNT_PT_EM);
        Assert.assertEquals(Long.valueOf(countInRedshift(BusinessEntity.Contact)), CONTACT_PA_EM);
        verifyTxnDailyStore(DAILY_TXN_DAYS_PT, MIN_TXN_DATE_PT, MAX_TXN_DATE_PT, //
                VERIFY_DAILYTXN_AMOUNT_PT, //
                VERIFY_DAILYTXN_QUANTITY_PT, //
                VERIFY_DAILYTXN_COST_PT);
    }

    @Override
    protected Map<BusinessEntity, Map<String, Object>> getExpectedReport() {
        Map<String, Object> accountReport = new HashMap<>();
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW,
                NEW_ACCOUNT_UT_EM);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, 0L);
        // FIXME: Currently UNMATCH is same as new Account which is wrong
        // accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UNMATCH, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        accountReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL,
                ACCOUNT_UT_EM);

        Map<String, Object> contactReport = new HashMap<>();
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, 0L);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, 0L);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        contactReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL,
                CONTACT_PA_EM);

        Map<String, Object> productReport = new HashMap<>();
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_ID, 0L);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_HIERARCHY,
                0L);
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
                TOTAL_PURCHASE_HISTORY_UT);

        Map<BusinessEntity, Map<String, Object>> expectedReport = new HashMap<>();
        expectedReport.put(BusinessEntity.Account, accountReport);
        expectedReport.put(BusinessEntity.Contact, contactReport);
        expectedReport.put(BusinessEntity.Product, productReport);
        expectedReport.put(BusinessEntity.Transaction, transactionReport);
        expectedReport.put(BusinessEntity.PurchaseHistory, purchaseHistoryReport);

        return expectedReport;
    }

    @Override
    protected Map<BusinessEntity, Long> getExpectedBatchStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_UT_EM);
        map.put(BusinessEntity.Contact, CONTACT_PA_EM);
        map.put(BusinessEntity.Product, BATCH_STORE_PRODUCT_PT);
        map.put(BusinessEntity.Transaction, DAILY_TXN_UT_EM);
        map.put(BusinessEntity.PeriodTransaction, PERIOD_TRANSACTION_UT_EM);
        return map;
    }

    @Override
    protected Map<BusinessEntity, Long> getExpectedRedshiftCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_UT_EM);
        map.put(BusinessEntity.Contact, CONTACT_PA_EM);
        return map;
    }

    @Override
    protected Map<BusinessEntity, Long> getExpectedServingStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_UT_EM);
        map.put(BusinessEntity.Contact, CONTACT_PA_EM);
        map.put(BusinessEntity.Product, SERVING_STORE_PRODUCTS_PT);
        map.put(BusinessEntity.ProductHierarchy, SERVING_STORE_PRODUCT_HIERARCHIES_PT);
        map.put(BusinessEntity.Transaction, DAILY_TXN_UT_EM);
        map.put(BusinessEntity.PeriodTransaction, PERIOD_TRANSACTION_UT_EM);
        return map;
    }
}
