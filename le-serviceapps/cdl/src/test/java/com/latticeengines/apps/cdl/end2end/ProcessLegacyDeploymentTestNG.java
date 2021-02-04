package com.latticeengines.apps.cdl.end2end;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.util.CuratedAttributeUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;

/**
 * Process Account, Contact, Product and Transaction for a new legacy tenant
 */
public class ProcessLegacyDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(ProcessLegacyDeploymentTestNG.class);
    private static final String ACCOUNT_FEED_TYPE = "AccountVDB";
    private static final String ACCOUNT_FEED_TYPE_NEW = "AccountVDB_New";
    private static final String CONTACT_FEED_TYPE = "ContactVDB";
    private static final String CONTACT_FEED_TYPE_NEW = "ContactVDB_New";
    private static final String PRODUCT_FEED_TYPE = "ProductVDB";
    private static final String TRANSACTION_FEED_TYPE = "TransactionVDB";
    static final String UNDER_SCORE = "_";

    @BeforeClass(groups = { "end2end" })
    @Override
    public void setup() throws Exception {
        // create VDB migration tenant
        HashMap<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.VDB_MIGRATION.getName(), true);
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName(), false);
        setupEnd2EndTestEnvironment(featureFlagMap);
    }

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        importData();
        initialVersion = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
        processAnalyzeSkipPublishToS3();
        verifyFirstProcess();
        upsertData();
        replaceData();
        processAnalyzeSkipPublishToS3();
        verifySecondProcess();
    }

    // Add sleep between each import to avoid 2 import jobs generate table
    // extract with same timestamp in second, then extract could overwrite
    // between each other
    protected void importData() throws InterruptedException {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());

        log.info("Importing accounts");
        mockVISIDBImport(BusinessEntity.Account, 1, ACCOUNT_FEED_TYPE);
        Thread.sleep(1100);

        log.info("Importing contacts");
        mockVISIDBImport(BusinessEntity.Contact, 1, CONTACT_FEED_TYPE);
        Thread.sleep(1100);

        log.info("Importing products");
        mockVISIDBImport(BusinessEntity.Product, 9, PRODUCT_FEED_TYPE);
        Thread.sleep(1100);

        log.info("Importing transactions");
        mockVISIDBImport(BusinessEntity.Transaction, 1, TRANSACTION_FEED_TYPE);
        Thread.sleep(2000);

        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    // Upsert accounts and contacts
    // Update 10 accounts, insert 10 new accounts
    // Update 10 contacts, insert 10 new contacts
    protected void upsertData() throws InterruptedException {
        log.info("Importing 20 accounts (10 for update, 10 for insert), by using new template");
        mockVISIDBImport(BusinessEntity.Account, 4, ACCOUNT_FEED_TYPE_NEW);
        Thread.sleep(1100);

        log.info("Importing 20 contacts (10 for update, 10 for insert), by using new template");
        mockVISIDBImport(BusinessEntity.Contact, 4, CONTACT_FEED_TYPE_NEW);
        Thread.sleep(1100);
    }

    // Replace product data by importing new product data
    // Replace transaction data
    // Delete all transaction data first, and then importing new data
    protected void replaceData() throws InterruptedException {
        log.info("Importing new products to replace existing products");
        mockVISIDBImport(BusinessEntity.Product, 10, PRODUCT_FEED_TYPE);
        Thread.sleep(1100);

        log.info("Register replacement action for transaction");
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        String email = MultiTenantContext.getEmailAddress();
        cdlProxy.cleanupAllByAction(customerSpace.toString(), BusinessEntity.Transaction, email);

        log.info("Importing new transaction data");
        mockVISIDBImport(BusinessEntity.Transaction, 2, TRANSACTION_FEED_TYPE);
        Thread.sleep(2000);
    }

    protected void verifyFirstProcess() {
        verifyProcess(true);
    }

    protected void verifySecondProcess() {
        verifyProcess(false);
    }

    protected void verifyProcess(boolean firstPA) {
        verifyProcessAccount(firstPA);
        createTestSegment3();
        verifySegmentCountsNonNegative(SEGMENT_NAME_3, Arrays.asList(BusinessEntity.Account, BusinessEntity.Contact));
        // segment count is different between 2 PA results, because one account's
        // SpendAnalyticsSegment value is updated in second PA which makes it not match
        // segment's account restriction
        Map<BusinessEntity, Long> segment3Counts = ImmutableMap.of(BusinessEntity.Account,
                firstPA ? SEGMENT_3_ACCOUNT_1 : SEGMENT_3_ACCOUNT_3, BusinessEntity.Contact,
                firstPA ? SEGMENT_3_CONTACT_1 : SEGMENT_3_CONTACT_3);
        verifyTestSegment3Counts(segment3Counts);

        // Create a test segment to verify proper behavior of the Curated Attributes
        // step and resulting table.
        createTestSegmentCuratedAttr();
        verifyTestSegmentCuratedAttrCounts(
                Collections.singletonMap(BusinessEntity.Account, firstPA ? ACCOUNT_PA : ACCOUNT_PA + NEW_ACCOUNT_PA));
    }

    void verifyProcessAccount(boolean firstPA) {
        verifyDataFeedStatus(DataFeed.Status.Active);
        verifyUpdateActions();

        if (firstPA) {
            verifyActiveVersion(DataCollection.Version.Green);
            verifyProcessAnalyzeReport(processAnalyzeAppId, getExpectedReport());
            verifyDataCollectionStatus(DataCollection.Version.Green);
        } else {
            verifyActiveVersion(DataCollection.Version.Blue);
            verifyProcessAnalyzeReport(processAnalyzeAppId, getUpsertExpectedReport());
            verifyDataCollectionStatus(DataCollection.Version.Blue);
        }
        verifyNumAttrsInAccount();
        verifyAccountFeatures();
        verifyAccountProfile();
        verifyDateAttrs();
        verifyNumberOfContacts();

        // Check that stats cubes only exist for the entities specified below.
        verifyStats(getEntitiesInStats());
        if (firstPA) {
            verifyBatchStore(getExpectedBatchStoreCounts());
            verifyServingStore(getExpectedServingStoreCounts());
            verifyExtraTableRoles(getExtraTableRoeCounts());
            verifyRedshift(getExpectedRedshiftCounts());
        } else {
            verifyBatchStore(getUpsertExpectedBatchStoreCounts());
            verifyServingStore(getUpsertExpectedServingStoreCounts());
            verifyExtraTableRoles(getUpsertExtraTableRoeCounts());
            verifyRedshift(getUpsertExpectedRedshiftCounts());
        }
    }

    private void verifyAccountProfile() {
        Table table1 = dataCollectionProxy.getTable(mainCustomerSpace, TableRoleInCollection.AccountProfile);
        Assert.assertNotNull(table1);
        Table table2 = dataCollectionProxy.getTable(mainCustomerSpace, TableRoleInCollection.LatticeAccountProfile);
        Assert.assertNotNull(table2);
    }

    private void verifyDateAttrs() {
        Table accountTable = dataCollectionProxy.getTable(mainCustomerSpace, BusinessEntity.Account.getBatchStore());
        Assert.assertNotNull(accountTable);
        Attribute attr1 = accountTable.getAttribute("user_Test_Date");
        Assert.assertNotNull(attr1);
        Assert.assertTrue(StringUtils.isNotBlank(attr1.getLastDataRefresh()), JsonUtils.serialize(attr1));

        Table contactTable = dataCollectionProxy.getTable(mainCustomerSpace, BusinessEntity.Contact.getServingStore());
        Assert.assertNotNull(contactTable);
        Attribute attr2 = contactTable.getAttribute("user_Last_Communication_Date");
        Assert.assertNotNull(attr2);
        Assert.assertTrue(StringUtils.isNotBlank(attr2.getLastDataRefresh()), JsonUtils.serialize(attr2));

        StatisticsContainer container = dataCollectionProxy.getStats(mainCustomerSpace, initialVersion.complement());
        Assert.assertNotNull(container);
        Map<String, StatsCube> cubes = container.getStatsCubes();
        Assert.assertTrue(MapUtils.isNotEmpty(cubes));

        Assert.assertTrue(cubes.containsKey(BusinessEntity.Account.name()));
        StatsCube accountCube = cubes.get(BusinessEntity.Account.name());
        verifyDateAttrStats(accountCube, "user_Test_Date");

        Assert.assertTrue(cubes.containsKey(BusinessEntity.Contact.name()));
        StatsCube contactCube = cubes.get(BusinessEntity.Contact.name());
        verifyDateAttrStats(contactCube, "user_Last_Communication_Date");
    }

    private void verifyDateAttrStats(StatsCube contactCube, String attrName) {
        Map<String, AttributeStats> attrStats = contactCube.getStatistics();
        Assert.assertTrue(MapUtils.isNotEmpty(attrStats));
        Assert.assertTrue(attrStats.containsKey(attrName));
        AttributeStats attrStat = attrStats.get(attrName);
        Assert.assertNotNull(attrStat.getBuckets());
        Assert.assertTrue(CollectionUtils.isNotEmpty(attrStat.getBuckets().getBucketList()));
        Bucket bucket = attrStat.getBuckets().getBucketList().get(0);
        Assert.assertTrue("Ever".equals(bucket.getLabel()) || //
                (bucket.getLabel().contains("Last") && bucket.getLabel().contains("Days")), //
                JsonUtils.serialize(attrStat));
    }

    private void verifyNumberOfContacts() {
        Table table = dataCollectionProxy.getTable(mainCustomerSpace, BusinessEntity.CuratedAccount.getServingStore());
        Assert.assertNotNull(table);
        Attribute attribute = table.getAttribute(InterfaceName.NumberOfContacts.name());
        Assert.assertNotNull(attribute);
        Assert.assertEquals(attribute.getCategory(), Category.CURATED_ACCOUNT_ATTRIBUTES.getName());
        Assert.assertEquals(attribute.getDisplayName(), CuratedAttributeUtils.NUMBER_OF_CONTACTS_DISPLAY_NAME);
        Assert.assertEquals(attribute.getFundamentalType(), FundamentalType.NUMERIC.getName());

        StatisticsContainer container = dataCollectionProxy.getStats(mainCustomerSpace, initialVersion.complement());
        Assert.assertNotNull(container);
        Map<String, StatsCube> cubes = container.getStatsCubes();
        Assert.assertTrue(MapUtils.isNotEmpty(cubes));
        Assert.assertTrue(cubes.containsKey(BusinessEntity.CuratedAccount.name()));
        StatsCube cube = cubes.get(BusinessEntity.CuratedAccount.name());
        Map<String, AttributeStats> attrStats = cube.getStatistics();
        Assert.assertTrue(MapUtils.isNotEmpty(attrStats));
        Assert.assertTrue(attrStats.containsKey(InterfaceName.NumberOfContacts.name()));
        AttributeStats attrStat = attrStats.get(InterfaceName.NumberOfContacts.name());
        Assert.assertNotNull(attrStat.getBuckets());
        Assert.assertTrue(CollectionUtils.isNotEmpty(attrStat.getBuckets().getBucketList()));
    }

    private void verifyNumAttrsInAccount() {
        String tableName = dataCollectionProxy.getTableName(mainCustomerSpace,
                BusinessEntity.Account.getServingStore());
        List<ColumnMetadata> cms = metadataProxy.getTableColumns(mainCustomerSpace, tableName);
        Assert.assertTrue(cms.size() < 20000, "Should not have more than 20000 account attributes");
    }

    protected BusinessEntity[] getEntitiesInStats() {
        return new BusinessEntity[] { BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.CuratedAccount,
                BusinessEntity.PurchaseHistory };
    }

    protected Map<BusinessEntity, Map<String, Object>> getExpectedReport() {
        Map<BusinessEntity, Map<String, Object>> expectedReport = new HashMap<>();
        expectedReport.put(BusinessEntity.Account, getAccountExpectedReport());
        expectedReport.put(BusinessEntity.Contact, getContactExpectedReport());
        expectedReport.put(BusinessEntity.Product, getProductExpectedReport());
        expectedReport.put(BusinessEntity.Transaction, getTransactionExpectedReport());
        expectedReport.put(BusinessEntity.PurchaseHistory, getPurchaseHistoryExpectedReport());

        return expectedReport;
    }

    protected Map<BusinessEntity, Map<String, Object>> getUpsertExpectedReport() {
        Map<BusinessEntity, Map<String, Object>> expectedReport = new HashMap<>();
        expectedReport.put(BusinessEntity.Account, getUpsertAccountExpectedReport());
        expectedReport.put(BusinessEntity.Contact, getUpsertContactExpectedReport());
        expectedReport.put(BusinessEntity.Product, getReplacedProductExpectedReport());
        expectedReport.put(BusinessEntity.Transaction, getReplacedTransactionExpectedReport());
        expectedReport.put(BusinessEntity.PurchaseHistory, getPurchaseHistoryExpectedReport());

        return expectedReport;
    }

    protected Map<String, Object> getAccountExpectedReport() {
        Map<String, Object> accountReport = new HashMap<>();
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.NEW,
                ACCOUNT_PA);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.UPDATE, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.UNMATCH, 6L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.DELETE, 0L);
        accountReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + UNDER_SCORE + ReportConstants.TOTAL, ACCOUNT_PA);
        return accountReport;
    }

    protected Map<String, Object> getUpsertAccountExpectedReport() {
        Map<String, Object> accountReport = new HashMap<>();
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.NEW,
                NEW_ACCOUNT_PA);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.UPDATE,
                UPDATE_ACCOUNT_PA);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.UNMATCH, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.DELETE, 0L);
        accountReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + UNDER_SCORE + ReportConstants.TOTAL, ACCOUNT_PA);
        return accountReport;
    }

    protected Map<String, Object> getContactExpectedReport() {
        Map<String, Object> contactReport = new HashMap<>();
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, CONTACT_PA);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, 0L);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        contactReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, CONTACT_PA);
        return contactReport;
    }

    protected Map<String, Object> getUpsertContactExpectedReport() {
        Map<String, Object> contactReport = new HashMap<>();
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, NEW_CONTACT_PA);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE,
                UPDATE_CONTACT_PA);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        contactReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, CONTACT_PA);
        return contactReport;
    }

    protected Map<String, Object> getProductExpectedReport() {
        Map<String, Object> productReport = new HashMap<>();
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_ID,
                PRODUCT_ID_VDB_PA);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_HIERARCHY,
                0L);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_BUNDLE, 0L);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.WARN_MESSAGE,
                PRODUCT_WARN_MESSAGE);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.ERROR_MESSAGE,
                PRODUCT_ERROR_MESSAGE);
        return productReport;
    }

    protected Map<String, Object> getReplacedProductExpectedReport() {
        Map<String, Object> productReport = new HashMap<>();
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_ID,
                NEW_PRODUCT_ID_VDB_PA);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_HIERARCHY,
                0L);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_BUNDLE, 0L);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.WARN_MESSAGE,
                PRODUCT_WARN_MESSAGE);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.ERROR_MESSAGE,
                PRODUCT_ERROR_MESSAGE);
        return productReport;
    }

    protected Map<String, Object> getTransactionExpectedReport() {
        Map<String, Object> transactionReport = new HashMap<>();
        transactionReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW,
                TRANSACTION_LEGACY_FIRST_PA);
        transactionReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        transactionReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL,
                TRANSACTION_LEGACY_FIRST_PA);
        return transactionReport;
    }

    protected Map<String, Object> getReplacedTransactionExpectedReport() {
        Map<String, Object> transactionReport = new HashMap<>();
        transactionReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW,
                TRANSACTION_LEGACY_SECOND_PA);
        transactionReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        transactionReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL,
                TRANSACTION_LEGACY_SECOND_PA);
        return transactionReport;
    }

    protected Map<String, Object> getPurchaseHistoryExpectedReport() {
        Map<String, Object> purchaseHistoryReport = new HashMap<>();
        purchaseHistoryReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL,
                TOTAL_PURCHASE_HISTORY_PA);
        return purchaseHistoryReport;
    }

    protected Map<BusinessEntity, Long> getExpectedBatchStoreCounts() {
        return ImmutableMap.of(//
                BusinessEntity.Account, ACCOUNT_PA, //
                BusinessEntity.Contact, CONTACT_PA, //
                BusinessEntity.Product, BATCH_STORE_PRODUCT_PA);
    }

    protected Map<BusinessEntity, Long> getUpsertExpectedBatchStoreCounts() {
        return ImmutableMap.of(//
                BusinessEntity.Account, ACCOUNT_PA + NEW_ACCOUNT_PA, //
                BusinessEntity.Contact, CONTACT_PA + NEW_CONTACT_PA, //
                BusinessEntity.Product, NEW_BATCH_STORE_PRODUCT_PA);
    }

    protected Map<BusinessEntity, Long> getExpectedServingStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_PA);
        map.put(BusinessEntity.Contact, CONTACT_PA);
        map.put(BusinessEntity.Product, SERVING_STORE_PRODUCTS_PA);
        return map;
    }

    protected Map<BusinessEntity, Long> getUpsertExpectedServingStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_PA + NEW_ACCOUNT_PA);
        map.put(BusinessEntity.Contact, CONTACT_PA + NEW_CONTACT_PA);
        map.put(BusinessEntity.Product, NEW_SERVING_STORE_PRODUCTS_PA);
        return map;
    }

    protected Map<TableRoleInCollection, Long> getExtraTableRoeCounts() {
        return ImmutableMap.of(TableRoleInCollection.AccountFeatures, ACCOUNT_PA);
    }

    protected Map<TableRoleInCollection, Long> getUpsertExtraTableRoeCounts() {
        return ImmutableMap.of(TableRoleInCollection.AccountFeatures, ACCOUNT_PA + NEW_ACCOUNT_PA);
    }

    protected Map<BusinessEntity, Long> getExpectedRedshiftCounts() {
        return ImmutableMap.of(//
                BusinessEntity.Account, ACCOUNT_PA, //
                BusinessEntity.Contact, CONTACT_PA);
    }

    protected Map<BusinessEntity, Long> getUpsertExpectedRedshiftCounts() {
        return ImmutableMap.of(//
                BusinessEntity.Account, ACCOUNT_PA + NEW_ACCOUNT_PA, //
                BusinessEntity.Contact, CONTACT_PA + NEW_CONTACT_PA);
    }
}
