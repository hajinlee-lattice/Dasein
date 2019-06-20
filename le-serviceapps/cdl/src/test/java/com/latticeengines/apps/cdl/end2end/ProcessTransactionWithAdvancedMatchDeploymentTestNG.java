package com.latticeengines.apps.cdl.end2end;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;

/**
 * Process Transaction imports after
 * ProcessAccountWithAdvancedMatchDeploymentTestNG with feature flag
 * EnableEntityMatch turned on
 */
public class ProcessTransactionWithAdvancedMatchDeploymentTestNG extends ProcessTransactionDeploymentTestNG {

    private static final Logger log = LoggerFactory
            .getLogger(ProcessTransactionWithAdvancedMatchDeploymentTestNG.class);

    static final String CHECK_POINT = "entitymatch_process2";

    @BeforeClass(groups = "end2end")
    @Override
    public void setup() throws Exception {
        log.info("Running setup with ENABLE_ENTITY_MATCH enabled!");
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName(), true);
        setupEnd2EndTestEnvironment(featureFlagMap);
        checkpointService
                .setPrecedingCheckpoints(Arrays.asList(ProcessAccountWithAdvancedMatchDeploymentTestNG.CHECK_POINT));
        log.info("Setup Complete!");
    }

    @Override
    protected void importData() throws Exception {
        mockCSVImport(BusinessEntity.Transaction, ADVANCED_MATCH_SUFFIX, 1, "DefaultSystem_TransactionData");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Transaction, ADVANCED_MATCH_SUFFIX, 2, "DefaultSystem_TransactionData");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Product, 1, "ProductBundle");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Product, 2, "ProductHierarchy");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Product, 3, "ProductVDB");
    }

    @Override
    protected String resumeFromCheckPoint() {
        return ProcessAccountWithAdvancedMatchDeploymentTestNG.CHECK_POINT;
    }

    @Override
    protected String saveToCheckPoint() {
        return CHECK_POINT;
    }

    @Override
    protected int expectedUserTestDateCntsBeforePA() {
        return 3;
    }

    @Override
    protected Map<BusinessEntity, Long> getExpectedBatchStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ENTITY_MATCH_TOTAL_ACCOUNT_P2);
        map.put(BusinessEntity.Contact, ENTITY_MATCH_CONTACT_P1);
        map.put(BusinessEntity.Product, BATCH_STORE_PRODUCT_P2);
        map.put(BusinessEntity.Transaction, ENTITY_MATCH_DAILY_TXN_P2);
        map.put(BusinessEntity.PeriodTransaction, ENTITY_MATCH_PERIOD_TXN_P2);
        return map;
    }

    @Override
    protected Map<BusinessEntity, Long> getExpectedServingStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ENTITY_MATCH_TOTAL_ACCOUNT_P2);
        map.put(BusinessEntity.Contact, ENTITY_MATCH_CONTACT_P1);
        map.put(BusinessEntity.Product, SERVING_STORE_PRODUCTS_P2);
        map.put(BusinessEntity.ProductHierarchy, SERVING_STORE_PRODUCT_HIERARCHIES_P2);
        map.put(BusinessEntity.Transaction, ENTITY_MATCH_DAILY_TXN_P2);
        map.put(BusinessEntity.PeriodTransaction, ENTITY_MATCH_PERIOD_TXN_P2);
        return map;
    }

    @Override
    protected Map<BusinessEntity, Long> getExpectedRedshiftCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ENTITY_MATCH_TOTAL_ACCOUNT_P2);
        map.put(BusinessEntity.Contact, ENTITY_MATCH_CONTACT_P1);
        return map;
    }

    @Override
    protected Map<BusinessEntity, Map<String, Object>> getExpectedReport() {
        Map<String, Object> accountReport = new HashMap<>();
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW,
                ENTITY_MATCH_NEW_ACCOUNT_P2);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        // FIXME: Currently UNMATCH is same as new Account which is wrong
        // accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UNMATCH, 0L);
        accountReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL,
                ENTITY_MATCH_TOTAL_ACCOUNT_P2);

        Map<String, Object> transactionReport = new HashMap<>();
        transactionReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW,
                NEW_TRANSACTION_P2);
        transactionReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        transactionReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL,
                NEW_TRANSACTION_P2);

        Map<String, Object> purchaseHistoryReport = new HashMap<>();
        purchaseHistoryReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL,
                TOTAL_PURCHASE_HISTORY_P2);

        Map<BusinessEntity, Map<String, Object>> expectedReport = new HashMap<>();
        expectedReport.put(BusinessEntity.Account, accountReport);
        expectedReport.put(BusinessEntity.Transaction, transactionReport);
        expectedReport.put(BusinessEntity.PurchaseHistory, purchaseHistoryReport);
        return expectedReport;
    }
}
