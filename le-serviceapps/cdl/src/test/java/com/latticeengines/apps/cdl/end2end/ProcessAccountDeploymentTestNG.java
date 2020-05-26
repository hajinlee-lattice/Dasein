package com.latticeengines.apps.cdl.end2end;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.cdl.workflow.steps.rebuild.CuratedAttributeUtils;
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
 * Process Account, Contact and Product for a new tenant
 */
public class ProcessAccountDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    static final String CHECK_POINT = "process1";
    static final String UNDER_SCORE = "_";

    @Value("${camille.zk.pod.id}")
    protected String podId;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        importData();
        initialVersion = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
        if (isLocalEnvironment()) {
            processAnalyzeSkipPublishToS3();
        } else {
            runTestWithRetry(getCandidateFailingSteps());
        }

        try {
            verifyProcess();
        } finally {
            if (isLocalEnvironment()) {
                saveCheckpoint(saveToCheckPoint());
            }
        }
    }

    // Add sleep between each import to avoid 2 import jobs generate table
    // extract with same timestamp in second, then extract could overwrite
    // between each other
    protected void importData() throws InterruptedException {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        mockCSVImport(BusinessEntity.Account, 1, "DefaultSystem_AccountData");
        Thread.sleep(1100);
        mockCSVImport(BusinessEntity.Contact, 1, "DefaultSystem_ContactData");
        Thread.sleep(1100);
        mockCSVImport(BusinessEntity.Product, 1, "ProductBundle");
        Thread.sleep(1100);
        mockCSVImport(BusinessEntity.Product, 2, "ProductHierarchy");
        Thread.sleep(1100);
        // TODO: (Yintao) should be changed to mock vdb import
        mockCSVImport(BusinessEntity.Product, 3, "ProductVDB");
        Thread.sleep(2000);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    protected void verifyProcess() {
        verifyProcessAccount();

        createTestSegment3();
        verifySegmentCountsNonNegative(SEGMENT_NAME_3, Arrays.asList(BusinessEntity.Account, BusinessEntity.Contact));
        Map<BusinessEntity, Long> segment3Counts = ImmutableMap.of(//
                BusinessEntity.Account, SEGMENT_3_ACCOUNT_1,
                BusinessEntity.Contact, SEGMENT_3_CONTACT_1);
        verifyTestSegment3Counts(segment3Counts);

        // Create a test segment to verify proper behavior of the Curated Attributes step and resulting table.
        createTestSegmentCuratedAttr();
        verifyTestSegmentCuratedAttrCounts(Collections.singletonMap(BusinessEntity.Account, ACCOUNT_PA));
    }

    void verifyProcessAccount() {
        verifyDataFeedStatus(DataFeed.Status.Active);
        verifyActiveVersion(DataCollection.Version.Green);
        verifyUpdateActions();

        verifyProcessAnalyzeReport(processAnalyzeAppId, getExpectedReport());
        verifyDataCollectionStatus(DataCollection.Version.Green);
        verifyNumAttrsInAccount();
        verifyAccountFeatures();
        verifyAccountProfile();
        verifyDateAttrs();
        verifyNumberOfContacts();

        // Check that stats cubes only exist for the entities specified below.
        verifyStats(getEntitiesInStats());
        verifyBatchStore(getExpectedBatchStoreCounts());
        verifyServingStore(getExpectedServingStoreCounts());
        verifyExtraTableRoles(getExtraTableRoeCounts());
        verifyRedshift(getExpectedRedshiftCounts());
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
        String tableName = dataCollectionProxy.getTableName(mainCustomerSpace, BusinessEntity.Account.getServingStore());
        List<ColumnMetadata> cms = metadataProxy.getTableColumns(mainCustomerSpace, tableName);
        Assert.assertTrue(cms.size() < 20000, "Should not have more than 20000 account attributes");
    }

    protected BusinessEntity[] getEntitiesInStats() {
        return new BusinessEntity[] { BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.CuratedAccount };
    }

    protected Map<BusinessEntity, Map<String, Object>> getExpectedReport() {
        Map<String, Object> accountReport = new HashMap<>();
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.NEW,
                ACCOUNT_PA);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.UPDATE, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.UNMATCH, 6L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.DELETE, 0L);
        accountReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + UNDER_SCORE + ReportConstants.TOTAL, ACCOUNT_PA);

        Map<String, Object> contactReport = new HashMap<>();
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, CONTACT_PA);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, 0L);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        contactReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, CONTACT_PA);

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

        Map<BusinessEntity, Map<String, Object>> expectedReport = new HashMap<>();
        expectedReport.put(BusinessEntity.Account, accountReport);
        expectedReport.put(BusinessEntity.Contact, contactReport);
        expectedReport.put(BusinessEntity.Product, productReport);

        return expectedReport;
    }

    protected Map<BusinessEntity, Long> getExpectedBatchStoreCounts() {
        return ImmutableMap.of(//
                BusinessEntity.Account, ACCOUNT_PA, //
                BusinessEntity.Contact, CONTACT_PA, //
                BusinessEntity.Product, BATCH_STORE_PRODUCT_PT);
    }

    protected Map<BusinessEntity, Long> getExpectedServingStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_PA);
        map.put(BusinessEntity.Contact, CONTACT_PA);
        map.put(BusinessEntity.Product, SERVING_STORE_PRODUCTS_PT);
        map.put(BusinessEntity.ProductHierarchy, SERVING_STORE_PRODUCT_HIERARCHIES_PT);
        return map;
    }

    protected Map<TableRoleInCollection, Long> getExtraTableRoeCounts() {
        return ImmutableMap.of(//
                TableRoleInCollection.AccountFeatures, ACCOUNT_PA, //
                TableRoleInCollection.AccountExport, ACCOUNT_PA //
        );
    }

    protected Map<BusinessEntity, Long> getExpectedRedshiftCounts() {
        return ImmutableMap.of(//
                BusinessEntity.Account, ACCOUNT_PA, //
                BusinessEntity.Contact, CONTACT_PA);
    }

    protected String saveToCheckPoint() {
        return CHECK_POINT;
    }

    protected List<String> getCandidateFailingSteps() {
        return Arrays.asList(
                "matchAccount", //
                "matchContact", //
                "entityMatchCheckpoint", //
                "mergeAccount", //
                "enrichAccount", //
                "enrichAccount", //
                "enrichAccount", //
                "profileAccount", //
                "profileAccount", //
                "profileAccount", //
                "profileAccount", //
                "profileAccount", //
                "generateBucketedAccount", //
                "generateBucketedAccount", //
                "generateBucketedAccount", //
                "generateBucketedAccount", //
                "generateBucketedAccount", //
                "mergeContact", //
                "profileContact", //
                "mergeProduct", //
                "profileProduct", //
                "profileProductHierarchy", //
                "combineStatistics", //
                "exportToRedshift", //
                "generateProcessingReport", // mimic failed in scoring
                "generateProcessingReport", //
                "generateProcessingReport", //
                "generateProcessingReport", //
                "generateProcessingReport", //
                "generateProcessingReport", //
                "generateProcessingReport", //
                "generateProcessingReport", //
                "generateProcessingReport", //
                "generateProcessingReport", //
                "exportProcessAnalyzeToS3", //
                "finishProcessing");
    }

}
