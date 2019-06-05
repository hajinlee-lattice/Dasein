package com.latticeengines.apps.cdl.end2end;

import java.io.IOException;
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
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.workflow.FailingStep;
import com.latticeengines.domain.exposed.workflow.JobStatus;
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
            runTestWithRetry();
        }

        try {
            verifyProcess();
        } finally {
            if (isLocalEnvironment()) {
                saveCheckpoint(saveToCheckPoint());
            }
        }
    }

    private void runTestWithRetry() {
        ProcessAnalyzeRequest request = new ProcessAnalyzeRequest();
        request.setSkipPublishToS3(isLocalEnvironment());
        FailingStep failingStep = new FailingStep();
        failingStep.setName("mergeContact");
        request.setFailingStep(failingStep);
        long start = System.currentTimeMillis();
        processAnalyze(request, JobStatus.FAILED);
        long duration1 = System.currentTimeMillis() - start;
        if (!isLocalEnvironment()) {
            wipeOutContractDirInHdfs();
        }
        start = System.currentTimeMillis();
        retryProcessAnalyze();
        long duration2 = System.currentTimeMillis() - start;
        if (isLocalEnvironment()) {
            // retry should be faster than the first attempt
            Assert.assertTrue(duration2 < duration1, //
                    "Duration of first and second PA are: " + duration1 + " and " + duration2);
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

    private void wipeOutContractDirInHdfs() {
        String contractPath = PathBuilder //
                .buildContractPath(podId, CustomerSpace.parse(mainCustomerSpace).getContractId()).toString();
        String tablesPath = PathBuilder //
                .buildDataTablePath(podId, CustomerSpace.parse(mainCustomerSpace)).toString();
        try {
            String filePath = tablesPath + "/File";
            String fileBkPath = contractPath + "/FileBackup";
            System.out.println("Backing up " + filePath);
            HdfsUtils.copyFiles(yarnConfiguration, filePath, fileBkPath);
            System.out.println("Wiping out " + tablesPath);
            HdfsUtils.rmdir(yarnConfiguration, tablesPath);
            System.out.println("Resuming " + filePath);
            HdfsUtils.copyFiles(yarnConfiguration, fileBkPath, filePath);
            Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, filePath));
            HdfsUtils.rmdir(yarnConfiguration, fileBkPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to wipe out hdfs dir.", e);
        }
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
        verifyTestSegmentCuratedAttrCounts(Collections.singletonMap(BusinessEntity.Account, ACCOUNT_1));
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
        Table table = dataCollectionProxy.getTable(mainCustomerSpace, BusinessEntity.Account.getBatchStore());
        Assert.assertNotNull(table);
        Attribute attribute = table.getAttribute("user_Test_Date");
        Assert.assertNotNull(attribute);
        Assert.assertTrue(StringUtils.isNotBlank(attribute.getLastDataRefresh()), JsonUtils.serialize(attribute));

        StatisticsContainer container = dataCollectionProxy.getStats(mainCustomerSpace, initialVersion.complement());
        Assert.assertNotNull(container);
        Map<String, StatsCube> cubes = container.getStatsCubes();
        Assert.assertTrue(MapUtils.isNotEmpty(cubes));
        Assert.assertTrue(cubes.containsKey(BusinessEntity.Account.name()));
        StatsCube cube = cubes.get(BusinessEntity.Account.name());
        Map<String, AttributeStats> attrStats = cube.getStatistics();
        Assert.assertTrue(MapUtils.isNotEmpty(attrStats));
        Assert.assertTrue(attrStats.containsKey("user_Test_Date"));
        AttributeStats attrStat = attrStats.get("user_Test_Date");
        Assert.assertNotNull(attrStat.getBuckets());
        Assert.assertTrue(CollectionUtils.isNotEmpty(attrStat.getBuckets().getBucketList()));
        Bucket bucket = attrStat.getBuckets().getBucketList().get(0);
        Assert.assertTrue("Ever".equals(bucket.getLabel()) || //
                (bucket.getLabel().contains("Last") && bucket.getLabel().contains("Days")), //
                JsonUtils.serialize(attrStat));
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
                ACCOUNT_1);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.UPDATE, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.UNMATCH, 6L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.DELETE, 0L);
        accountReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + UNDER_SCORE + ReportConstants.TOTAL, ACCOUNT_1);

        Map<String, Object> contactReport = new HashMap<>();
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, CONTACT_1);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, 0L);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        contactReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, CONTACT_1);

        Map<String, Object> productReport = new HashMap<>();
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_ID,
                PRODUCT_ID);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_HIERARCHY,
                PRODUCT_HIERARCHY);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_BUNDLE,
                PRODUCT_BUNDLE);
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
                BusinessEntity.Account, ACCOUNT_1, //
                BusinessEntity.Contact, CONTACT_1, //
                BusinessEntity.Product, BATCH_STORE_PRODUCTS);
    }

    protected Map<BusinessEntity, Long> getExpectedServingStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_1);
        map.put(BusinessEntity.Contact, CONTACT_1);
        map.put(BusinessEntity.Product, SERVING_STORE_PRODUCTS);
        map.put(BusinessEntity.ProductHierarchy, SERVING_STORE_PRODUCT_HIERARCHIES);
        return map;
    }

    protected Map<TableRoleInCollection, Long> getExtraTableRoeCounts() {
        return ImmutableMap.of(//
                TableRoleInCollection.AccountFeatures, ACCOUNT_1, //
                TableRoleInCollection.AccountExport, ACCOUNT_1 //
        );
    }

    protected Map<BusinessEntity, Long> getExpectedRedshiftCounts() {
        return ImmutableMap.of(//
                BusinessEntity.Account, ACCOUNT_1, //
                BusinessEntity.Contact, CONTACT_1);
    }

    protected String saveToCheckPoint() {
        return CHECK_POINT;
    }

}
