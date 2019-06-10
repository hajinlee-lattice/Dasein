package com.latticeengines.apps.cdl.end2end;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.query.BusinessEntity;

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
        map.put(BusinessEntity.Account, ENTITY_MATCH_ACCOUNT_4);
        map.put(BusinessEntity.Contact, ENTITY_MATCH_CONTACT_1);
        map.put(BusinessEntity.Product, BATCH_STORE_PRODUCTS);
        map.put(BusinessEntity.Transaction, TRANSACTION_1);
        map.put(BusinessEntity.PeriodTransaction, PERIOD_TRANSACTION_1);
        return map;
    }

    @Override
    protected Map<BusinessEntity, Long> getExpectedServingStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ENTITY_MATCH_ACCOUNT_4);
        map.put(BusinessEntity.Contact, ENTITY_MATCH_CONTACT_1);
        map.put(BusinessEntity.Product, SERVING_STORE_PRODUCTS);
        map.put(BusinessEntity.ProductHierarchy, SERVING_STORE_PRODUCT_HIERARCHIES);
        map.put(BusinessEntity.Transaction, TRANSACTION_1);
        map.put(BusinessEntity.PeriodTransaction, PERIOD_TRANSACTION_1);
        return map;
    }

    @Override
    protected Map<BusinessEntity, Long> getExpectedRedshiftCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ENTITY_MATCH_ACCOUNT_4);
        map.put(BusinessEntity.Contact, ENTITY_MATCH_CONTACT_1);
        return map;
    }
}
