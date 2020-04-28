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
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.cdl.workflow.steps.rebuild.CuratedAccountAttributesStep;
import com.latticeengines.common.exposed.util.JsonUtils;
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
    static final String CHECK_POINT = "process1";
    static final String UNDER_SCORE = "_";
    // Number of products in batch store after first PA for legacy tenant
    static final Long BATCH_STORE_PRODUCT = 83L;
    // Number of products in serving store after first PA for legacy tenant
    static final Long SERVING_STORE_PRODUCTS = 14L;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        importData();
        initialVersion = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
        processAnalyzeSkipPublishToS3();
        verifyFirstProcess();
        upsertData();
        processAnalyzeSkipPublishToS3();
        verifySecondProcess();
    }

    // Add sleep between each import to avoid 2 import jobs generate table
    // extract with same timestamp in second, then extract could overwrite
    // between each other
    protected void importData() throws InterruptedException {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());

        log.info("Importing new accounts");
        mockCSVImport(BusinessEntity.Account, 1, "DefaultSystem_AccountData");
        Thread.sleep(1100);

        log.info("Importing new contacts");
        mockCSVImport(BusinessEntity.Contact, 1, "DefaultSystem_ContactData");
        Thread.sleep(1100);

        log.info("Importing new products");
        mockCSVImport(BusinessEntity.Product, 1, "ProductBundle");
        Thread.sleep(1100);

        log.info("Importing new product hierarchy");
        mockCSVImport(BusinessEntity.Product, 2, "ProductHierarchy");
        Thread.sleep(1100);

        log.info("Importing new transactions");
        mockCSVImport(BusinessEntity.Transaction, 1, "DefaultSystem_TransactionData");
        Thread.sleep(2000);

        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    // Upsert accounts and contacts
    // Update 10 accounts, insert 10 new accounts
    // Update 10 contacts, insert 10 new contacts
    protected void upsertData() throws InterruptedException {
        log.info("Importing 20 accounts (10 for update, 10 for insert)");
        mockCSVImport(BusinessEntity.Account, 4, "DefaultSystem_AccountData");
        Thread.sleep(1100);

        log.info("Importing 20 contacts (10 for update, 10 for insert)");
        mockCSVImport(BusinessEntity.Contact, 4, "DefaultSystem_ContactData");
        Thread.sleep(1100);
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
        Map<BusinessEntity, Long> segment3Counts = ImmutableMap.of(//
                BusinessEntity.Account, SEGMENT_3_ACCOUNT_1, BusinessEntity.Contact, SEGMENT_3_CONTACT_1);
        verifyTestSegment3Counts(segment3Counts);

        // Create a test segment to verify proper behavior of the Curated Attributes
        // step and resulting table.
        createTestSegmentCuratedAttr();
        verifyTestSegmentCuratedAttrCounts(Collections.singletonMap(BusinessEntity.Account, ACCOUNT_PA));
    }

    void verifyProcessAccount(boolean firstPA) {
        verifyDataFeedStatus(DataFeed.Status.Active);
        verifyActiveVersion(DataCollection.Version.Green);
        verifyUpdateActions();

        if (firstPA) {
            verifyProcessAnalyzeReport(processAnalyzeAppId, getExpectedReport());
            verifyDataCollectionStatus(DataCollection.Version.Green);
        } else {
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
        Table table = dataCollectionProxy.getTable(mainCustomerSpace, TableRoleInCollection.Profile);
        Assert.assertNotNull(table);
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
        Assert.assertEquals(attribute.getDisplayName(), CuratedAccountAttributesStep.NUMBER_OF_CONTACTS_DISPLAY_NAME);
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

        return expectedReport;
    }

    protected Map<BusinessEntity, Map<String, Object>> getUpsertExpectedReport() {
        Map<BusinessEntity, Map<String, Object>> expectedReport = new HashMap<>();
        expectedReport.put(BusinessEntity.Account, getUpsertAccountExpectedReport());
        expectedReport.put(BusinessEntity.Contact, getUpsertContactExpectedReport());
        expectedReport.put(BusinessEntity.Product, getProductExpectedReport());

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
                ACCOUNT_PA_NEW_ADD);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.UPDATE,
                ACCOUNT_PA_UPDATE);
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
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW,
                CONTACT_PA_NEW_ADD);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE,
                CONTACT_PA_UPDATE);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        contactReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, CONTACT_PA);
        return contactReport;
    }

    protected Map<String, Object> getProductExpectedReport() {
        Map<String, Object> productReport = new HashMap<>();
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_ID,
                PRODUCT_ID_PA);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_HIERARCHY,
                PRODUCT_HIERARCHY_PA);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_BUNDLE,
                PRODUCT_BUNDLE_PA);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.WARN_MESSAGE,
                PRODUCT_WARN_MESSAGE);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.ERROR_MESSAGE,
                PRODUCT_ERROR_MESSAGE);
        return productReport;
    }

    protected Map<BusinessEntity, Long> getExpectedBatchStoreCounts() {
        return ImmutableMap.of(//
                BusinessEntity.Account, ACCOUNT_PA, //
                BusinessEntity.Contact, CONTACT_PA, //
                BusinessEntity.Product, BATCH_STORE_PRODUCT);
    }

    protected Map<BusinessEntity, Long> getUpsertExpectedBatchStoreCounts() {
        return ImmutableMap.of(//
                BusinessEntity.Account, ACCOUNT_PA + ACCOUNT_PA_NEW_ADD, //
                BusinessEntity.Contact, CONTACT_PA + CONTACT_PA_NEW_ADD, //
                BusinessEntity.Product, BATCH_STORE_PRODUCT);
    }

    protected Map<BusinessEntity, Long> getExpectedServingStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_PA);
        map.put(BusinessEntity.Contact, CONTACT_PA);
        map.put(BusinessEntity.Product, SERVING_STORE_PRODUCTS);
        map.put(BusinessEntity.ProductHierarchy, SERVING_STORE_PRODUCT_HIERARCHIES_PT);
        return map;
    }

    protected Map<BusinessEntity, Long> getUpsertExpectedServingStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_PA + ACCOUNT_PA_NEW_ADD);
        map.put(BusinessEntity.Contact, CONTACT_PA + CONTACT_PA_NEW_ADD);
        map.put(BusinessEntity.Product, SERVING_STORE_PRODUCTS);
        map.put(BusinessEntity.ProductHierarchy, SERVING_STORE_PRODUCT_HIERARCHIES_PT);
        return map;
    }

    protected Map<TableRoleInCollection, Long> getExtraTableRoeCounts() {
        return ImmutableMap.of(//
                TableRoleInCollection.AccountFeatures, ACCOUNT_PA, //
                TableRoleInCollection.AccountExport, ACCOUNT_PA //
        );
    }

    protected Map<TableRoleInCollection, Long> getUpsertExtraTableRoeCounts() {
        return ImmutableMap.of(//
                TableRoleInCollection.AccountFeatures, ACCOUNT_PA + ACCOUNT_PA_NEW_ADD, //
                TableRoleInCollection.AccountExport, ACCOUNT_PA + CONTACT_PA_NEW_ADD //
        );
    }

    protected Map<BusinessEntity, Long> getExpectedRedshiftCounts() {
        return ImmutableMap.of(//
                BusinessEntity.Account, ACCOUNT_PA, //
                BusinessEntity.Contact, CONTACT_PA);
    }

    protected Map<BusinessEntity, Long> getUpsertExpectedRedshiftCounts() {
        return ImmutableMap.of(//
                BusinessEntity.Account, ACCOUNT_PA + ACCOUNT_PA_NEW_ADD, //
                BusinessEntity.Contact, CONTACT_PA + CONTACT_PA_NEW_ADD);
    }
}
