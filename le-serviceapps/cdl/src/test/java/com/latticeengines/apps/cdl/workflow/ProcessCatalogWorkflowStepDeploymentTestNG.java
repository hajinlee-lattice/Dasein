package com.latticeengines.apps.cdl.workflow;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedCatalog;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLWorkflowFrameworkDeploymentTestNGBase;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.cdl.activity.ActivityImport;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask.IngestionBehavior;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessCatalogWorkflowConfiguration;
import com.latticeengines.proxy.exposed.cdl.ActivityStoreProxy;

/*
 * dpltc deploy -a pls,cdl,admin,metadata,workflowapi,datacloudapi
 */
public class ProcessCatalogWorkflowStepDeploymentTestNG extends CDLWorkflowFrameworkDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ProcessCatalogWorkflowStepDeploymentTestNG.class);

    // use account file to fake import for all catalog for now
    private static final BusinessEntity TEST_FILE_ENTITY = Account;
    private static final EntityType TEST_FILE_ENTITY_TYPE = EntityType.Accounts;
    private static final String ADVANCED_MATCH_SUFFIX = "EntityMatch";
    private static final long NUM_ACCOUNTS_FILE1 = 900L;
    private static final long NUM_ACCOUNTS_FILE3 = 100L;

    private static final String CATALOG_1 = "Catalog1";
    private static final String CATALOG_2 = "Catalog2";
    private static final String CATALOG_3 = "Catalog3";

    @Inject
    private ActivityStoreProxy activityStoreProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        log.info("Running setup with ENABLE_ENTITY_MATCH_GA enabled!");
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), true);
        setupEnd2EndTestEnvironment(featureFlagMap);
        /*-
         * uncomment following line to debug
         */
//        testBed.excludeTestTenantsForCleanup(singletonList(mainTestTenant));
        log.info("Setup Complete!");
    }

    @Override
    @Test(groups = "deployment")
    public void testWorkflow() throws Exception {
        // change status to initialized to allow register extracts
        dataFeedProxy.updateDataFeedStatus(mainCustomerSpace, DataFeed.Status.Initialized.getName());

        /*-
         * empty universe
         * catalog 1 merge two files with same content together, should be deduped correctly
         * catalog 2 merge two files with diff content
         * catalog 3 with only one file
         */
        Map<String, IngestionBehavior> behaviors = new HashMap<>();
        Map<String, String> primaryKeys = new HashMap<>();
        behaviors.put(CATALOG_1, IngestionBehavior.Upsert);
        behaviors.put(CATALOG_2, IngestionBehavior.Replace);
        behaviors.put(CATALOG_3, IngestionBehavior.Upsert);
        primaryKeys.put(CATALOG_1, InterfaceName.CustomerAccountId.name());
        primaryKeys.put(CATALOG_2, InterfaceName.CustomerAccountId.name());
        primaryKeys.put(CATALOG_3, InterfaceName.CustomerAccountId.name());

        Map<String, List<ActivityImport>> catalogImports = new HashMap<>();
        catalogImports.put(CATALOG_1, prepareCatalogImports(CATALOG_1, new int[] { 1, 1 }, IngestionBehavior.Upsert,
                InterfaceName.CustomerAccountId.name()));
        catalogImports.put(CATALOG_2, prepareCatalogImports(CATALOG_2, new int[] { 1, 3 }, IngestionBehavior.Replace,
                InterfaceName.CustomerAccountId.name()));
        catalogImports.put(CATALOG_3, prepareCatalogImports(CATALOG_3, new int[] { 1 }, IngestionBehavior.Upsert,
                InterfaceName.CustomerAccountId.name()));
        // catalogName -> catalogId
        Map<String, String> catalogIdMap = catalogImports.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get(0).getUniqueId()));
        ArrayList<String> catalogIds = new ArrayList<>(catalogIdMap.values());
        importCatalogAndBuildBatchStore(behaviors, primaryKeys, catalogImports, Collections.emptyMap(), catalogIdMap);

        // verify inactive tables
        Map<String, String> tableNamesFirstRun = dataCollectionProxy.getTableNamesWithSignatures(mainCustomerSpace,
                ConsolidatedCatalog, inactive, catalogIds);
        log.info("Catalog batch store tables in first run {}, inactive version = {}", tableNamesFirstRun,
                inactive.name());
        Assert.assertNotNull(tableNamesFirstRun);
        catalogIds.forEach(catalogId -> Assert.assertNotNull(tableNamesFirstRun.get(catalogId),
                String.format("Should have catalog batch store table for catalog %s in inactive version %s",
                        catalogId, inactive.name())));
        // make sure records are merged correctly
        Assert.assertEquals(countRecordsInTable(tableNamesFirstRun.get(catalogIdMap.get(CATALOG_1))),
                NUM_ACCOUNTS_FILE1);
        Assert.assertEquals(countRecordsInTable(tableNamesFirstRun.get(catalogIdMap.get(CATALOG_2))),
                NUM_ACCOUNTS_FILE1 + NUM_ACCOUNTS_FILE3);
        Assert.assertEquals(countRecordsInTable(tableNamesFirstRun.get(catalogIdMap.get(CATALOG_3))),
                NUM_ACCOUNTS_FILE1);

        /*-
         * switch version and clear inactive,
         * catalog 1 & 2 has another import (check upsert/replace) behavior.
         * catalog 3 has no import (make sure active table is correctly linked)
         */
        swapCDLVersions();
        clearAllTables(inactive);

        catalogImports.clear();
        catalogImports.put(CATALOG_1, prepareCatalogImports(CATALOG_1, new int[] { 1 }, IngestionBehavior.Upsert,
                InterfaceName.CustomerAccountId.name()));
        catalogImports.put(CATALOG_2, prepareCatalogImports(CATALOG_2, new int[] { 1 }, IngestionBehavior.Replace,
                InterfaceName.CustomerAccountId.name()));
        importCatalogAndBuildBatchStore(behaviors, primaryKeys, catalogImports, tableNamesFirstRun, catalogIdMap);

        // verify
        Map<String, String> tableNamesSecondRun = dataCollectionProxy.getTableNamesWithSignatures(mainCustomerSpace,
                ConsolidatedCatalog, inactive, catalogIds);
        log.info("Catalog batch store tables in second run {}, inactive version = {}", tableNamesSecondRun,
                inactive.name());
        Assert.assertNotNull(tableNamesSecondRun);
        catalogIds.forEach(catalogId -> Assert.assertNotNull(tableNamesSecondRun.get(catalogId),
                String.format("Should have catalog batch store table for catalog %s in inactive version %s",
                        catalogId, inactive.name())));
        Assert.assertNotEquals(tableNamesFirstRun.get(catalogIdMap.get(CATALOG_1)),
                tableNamesSecondRun.get(catalogIdMap.get(CATALOG_1)));
        Assert.assertNotEquals(tableNamesFirstRun.get(catalogIdMap.get(CATALOG_2)),
                tableNamesSecondRun.get(catalogIdMap.get(CATALOG_2)));
        Assert.assertEquals(tableNamesFirstRun.get(catalogIdMap.get(CATALOG_3)),
                tableNamesSecondRun.get(catalogIdMap.get(CATALOG_3)),
                String.format("Catalog %s has no import and should be linked with table in old version", CATALOG_3));

        // make sure records are merged correctly
        Assert.assertEquals(countRecordsInTable(tableNamesSecondRun.get(catalogIdMap.get(CATALOG_1))),
                NUM_ACCOUNTS_FILE1);
        // catalog2 is replace so should contain NUM_ACCOUNTS_FILE1
        Assert.assertEquals(countRecordsInTable(tableNamesSecondRun.get(catalogIdMap.get(CATALOG_2))),
                NUM_ACCOUNTS_FILE1);
        Assert.assertEquals(countRecordsInTable(tableNamesSecondRun.get(catalogIdMap.get(CATALOG_3))),
                NUM_ACCOUNTS_FILE1);

        /*-
         * No imports for all catalog, make sure existing tables are copied
         */
        swapCDLVersions();
        clearAllTables(inactive);
        catalogImports.clear();
        importCatalogAndBuildBatchStore(behaviors, primaryKeys, catalogImports, tableNamesSecondRun, catalogIdMap);

        // verify tables are linked to inactive version
        Map<String, String> tableNamesNoImportRun = dataCollectionProxy.getTableNamesWithSignatures(mainCustomerSpace,
                ConsolidatedCatalog, inactive, catalogIds);
        Assert.assertEquals(tableNamesNoImportRun, tableNamesSecondRun);
    }

    @Override
    protected void verifyTest() {
        // noop, cannot use Wrapper workflow in remote for now due to deserialization
        // problem of config.
        // TODO enhance later
    }

    private void importCatalogAndBuildBatchStore(@NotNull Map<String, IngestionBehavior> behaviors,
            @NotNull Map<String, String> primaryKeys, @NotNull Map<String, List<ActivityImport>> catalogImports,
            @NotNull Map<String, String> activeBatchStoreTables, @NotNull Map<String, String> catalogIdMap)
            throws Exception {
        ProcessCatalogWorkflowConfiguration config = new ProcessCatalogWorkflowConfiguration.Builder() //
                .customer(mainTestCustomerSpace) //
                .catalogIngestionBehaviors(changeKeyFromNameToId(behaviors, catalogIdMap)) //
                .catalogTables(activeBatchStoreTables) //
                .catalogPrimaryKeyColumns(changeKeyFromNameToId(primaryKeys, catalogIdMap)) //
                .catalogImports(changeKeyFromNameToId(catalogImports, catalogIdMap)) //
                .build();
        setCDLVersions(config, active);
        skipPublishToS3(config);
        runWorkflowRemote(config);
    }

    private <V> Map<String, V> changeKeyFromNameToId(@NotNull Map<String, V> nameMap,
            @NotNull Map<String, String> nameIdMap) {
        return nameMap.entrySet().stream()
                .collect(Collectors.toMap(entry -> nameIdMap.get(entry.getKey()), Map.Entry::getValue));
    }

    private List<ActivityImport> prepareCatalogImports(@NotNull String catalogName, @NotNull int[] fileIndices,
            @NotNull IngestionBehavior behavior, String primaryKeyColumn) {
        String testArtifactFeedType = getFeedTypeByEntity(TEST_FILE_ENTITY_TYPE);
        String testFeedType = catalogFeedType(catalogName);
        DataFeedTask dataFeedTask = registerMockDataFeedTask(Account, ADVANCED_MATCH_SUFFIX, testArtifactFeedType,
                testFeedType, behavior);

        // fake import from test artifact files and retrieve table names
        List<String> tables = Arrays.stream(fileIndices) //
                .mapToObj(idx -> {
                    try {
                        List<String> tableNames = mockCSVImport(TEST_FILE_ENTITY, ADVANCED_MATCH_SUFFIX, idx,
                                dataFeedTask);
                        Thread.sleep(2000L);
                        return tableNames;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }) //
                .flatMap(List::stream) //
                .collect(Collectors.toList());

        // upsert catalog
        Catalog catalog = activityStoreProxy.findCatalogByName(mainCustomerSpace, catalogName);
        if (catalog == null) {
            catalog = activityStoreProxy.createCatalog(mainCustomerSpace, catalogName, dataFeedTask.getUniqueId(),
                    primaryKeyColumn);
        }
        String catalogId = catalog.getCatalogId();

        List<ActivityImport> catalogImports = tables.stream() //
                .map(tableName -> {
                    ActivityImport catalogImport = new ActivityImport();
                    catalogImport.setUniqueId(catalogId);
                    catalogImport.setTableName(tableName);
                    catalogImport.setOriginalFilename(tableName + ".csv");
                    return catalogImport;
                }) //
                .collect(Collectors.toList());

        log.info("Prepared catalog imports {} for catalog {}(PID={}) using fileIdx={}", catalogImports, catalogName,
                catalog.getPid(), Arrays.toString(fileIndices));
        return catalogImports;
    }

    private String catalogFeedType(String catalogName) {
        return String.format("CatalogFeed_%s", catalogName);
    }

}
