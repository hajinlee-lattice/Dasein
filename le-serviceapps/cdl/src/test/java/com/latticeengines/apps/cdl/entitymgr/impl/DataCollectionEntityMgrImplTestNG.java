package com.latticeengines.apps.cdl.entitymgr.impl;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedDailyTransaction;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedProduct;
import static org.testng.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.persistence.PersistenceException;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.cdl.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.StatisticsContainerEntityMgr;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionTable;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.testframework.service.impl.SimpleRetryAnalyzer;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({ SimpleRetryListener.class })
public class DataCollectionEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DataCollectionEntityMgrImplTestNG.class);

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Autowired
    private TableEntityMgr tableEntityMgr;

    @Autowired
    private StatisticsContainerEntityMgr statisticsContainerEntityMgr;

    @Autowired
    private SegmentService segmentService;

    private static final String tableName1 = "Table1";
    private static final String tableName2 = "Table2";
    private static final String tableName3 = "Table3";

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
        tableEntityMgr.deleteByName(tableName1);
        tableEntityMgr.deleteByName(tableName2);

    }

    @Test(groups = "functional")
    public void create() {
        DataCollection collection = dataCollectionEntityMgr.getDataCollection(collectionName);
        Assert.assertEquals(collection.getName(), collectionName);
    }

    @Test(groups = "functional", dependsOnMethods = "create", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testUpsertDataCollectionTable() throws Exception {
        DataCollection.Version version = dataCollectionEntityMgr.findActiveVersion();
        TableRoleInCollection role = ConsolidatedProduct;
        String signature = NamingUtils.randomSuffix("sig", 5);
        Table table1 = testTable();
        Table table2 = testTable();
        log.info("Version={}, Role={}, Signature={}, Table1={}, Table2={}", version, role, signature, table1.getName(),
                table2.getName());

        DataCollectionTable dcTable = dataCollectionEntityMgr.upsertTableToCollection(collectionName, table1.getName(),
                role, signature, version);
        Thread.sleep(1000L); // deal with replication lag
        validateDataCollectionTable(dcTable, null, table1.getName(), version, role, signature);
        // get pid of created table
        Long pid = dcTable.getPid();
        Assert.assertNotNull(pid);
        log.info("DataCollectionTable PID = {}", pid);
        // validate with getByPid
        dcTable = dataCollectionEntityMgr.findDataCollectionTableByPid(pid);
        validateDataCollectionTable(dcTable, pid, table1.getName(), version, role, signature);

        // update with another table
        dcTable = dataCollectionEntityMgr.upsertTableToCollection(collectionName, table2.getName(), role, signature,
                version);
        Thread.sleep(1000L); // deal with replication lag
        validateDataCollectionTable(dcTable, pid, table2.getName(), version, role, signature);
        dcTable = dataCollectionEntityMgr.findDataCollectionTableByPid(pid);
        validateDataCollectionTable(dcTable, pid, table2.getName(), version, role, signature);

        // test find by [ collection, role, version, signature ]
        Map<String, Table> tables = dataCollectionEntityMgr.findTablesOfRoleAndSignatures(collectionName, role, version,
                Collections.singleton(signature));
        Assert.assertNotNull(tables);
        Assert.assertEquals(tables.size(), 1);
        Assert.assertTrue(tables.containsKey(signature),
                String.format("Query result should contains signature %s", signature));
        Assert.assertNotNull(tables.get(signature));
        Assert.assertEquals(tables.get(signature).getName(), table2.getName());
    }

    /*-
     * [ collection, role, version, signature ] needs to be unique if all not null
     */
    @Test(groups = "functional", dependsOnMethods = "create", expectedExceptions = { PersistenceException.class })
    private void testSignatureConflict() {
        DataCollection.Version version = dataCollectionEntityMgr.findActiveVersion();
        createTwoCollectionTables(ConsolidatedProduct, version, NamingUtils.randomSuffix("sig", 5));
    }

    /*
     * null signature won't cause unique constraint violation
     */
    @Test(groups = "functional", dependsOnMethods = "create")
    private void testNullSignature() {
        DataCollection.Version version = dataCollectionEntityMgr.findActiveVersion();
        Pair<DataCollectionTable, DataCollectionTable> result = createTwoCollectionTables(ConsolidatedDailyTransaction,
                version, null);
        Assert.assertNotNull(result);
        Assert.assertNotNull(result.getLeft());
        Assert.assertNotNull(result.getRight());
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void findTableByRole() {
        DataCollection.Version version = dataCollectionEntityMgr.findActiveVersion();
        Table table1 = new Table();
        table1.setName(tableName1);
        table1.setDisplayName(tableName1);
        table1.setTableType(TableType.DATATABLE);
        tableEntityMgr.create(table1);
        dataCollectionEntityMgr.upsertTableToCollection(collectionName, tableName1, TableRoleInCollection.ConsolidatedAccount, version);

        Table table2 = new Table();
        table2.setName(tableName2);
        table2.setDisplayName(tableName2);
        table2.setTableType(TableType.DATATABLE);
        tableEntityMgr.create(table2);
        dataCollectionEntityMgr.upsertTableToCollection(collectionName, tableName2, TableRoleInCollection.Profile, version);

        List<Table> tables = dataCollectionEntityMgr.findTablesOfRole(collectionName, TableRoleInCollection.ConsolidatedAccount, version);
        Assert.assertNotNull(tables);
        Assert.assertEquals(tables.size(), 1);
        Assert.assertEquals(tables.get(0).getName(), tableName1);

        tables = dataCollectionEntityMgr.findTablesOfRole(collectionName, TableRoleInCollection.Profile, version);
        Assert.assertNotNull(tables);
        Assert.assertEquals(tables.size(), 1);
        Assert.assertEquals(tables.get(0).getName(), tableName2);
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void inactiveVersion() {
        DataCollection.Version inactiveVersion = dataCollectionEntityMgr.findInactiveVersion();
        Table table3 = new Table();
        table3.setName(tableName3);
        table3.setDisplayName(tableName3);
        table3.setTableType(TableType.DATATABLE);
        tableEntityMgr.create(table3);
        dataCollectionEntityMgr.upsertTableToCollection(collectionName, tableName3, TableRoleInCollection.ConsolidatedContact, inactiveVersion);

        DataCollection.Version version = dataCollectionEntityMgr.findActiveVersion();
        List<Table> tables = dataCollectionEntityMgr.findTablesOfRole(collectionName, TableRoleInCollection.ConsolidatedContact, version);
        Assert.assertNotNull(tables);
        Assert.assertTrue(tables.isEmpty());

        tables = dataCollectionEntityMgr.findTablesOfRole(collectionName, TableRoleInCollection.ConsolidatedContact, inactiveVersion);
        Assert.assertNotNull(tables);
        Assert.assertEquals(tables.size(), 1);
        Assert.assertEquals(tables.get(0).getName(), tableName3);
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void checkMasterSegment() {
        DataCollection.Version version = dataCollectionEntityMgr.findActiveVersion();
        MetadataSegment masterSegment = segmentService.findMaster(collectionName);
        Assert.assertNotNull(masterSegment);

        StatisticsContainer statisticsContainer = new StatisticsContainer();
        statisticsContainer.setStatsCubes(ImmutableMap.of("Account", new StatsCube()));
        statisticsContainer.setVersion(version);
        dataCollectionEntityMgr.upsertStatsForMasterSegment(collectionName, statisticsContainer);

        StatisticsContainer retrieved = statisticsContainerEntityMgr.findInMasterSegment(collectionName, version);
        Assert.assertNotNull(retrieved);
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void retrieve() {
        DataCollection retrieved = dataCollectionEntityMgr.getDataCollection(collectionName);
        assertEquals(retrieved.getName(), collectionName);
    }

    @Test(groups = "functional", dependsOnMethods = "retrieve")
    public void recreate() {
        DataCollection retrieved = dataCollectionEntityMgr.getDataCollection(collectionName);
        String newCollectionName = NamingUtils.timestamp("DC");
        retrieved.setName(newCollectionName);
        dataCollectionEntityMgr.createOrUpdate(retrieved);

        retrieved.setName(collectionName);
        dataCollectionEntityMgr.createOrUpdate(retrieved);
    }

    private Pair<DataCollectionTable, DataCollectionTable> createTwoCollectionTables(
            @NotNull TableRoleInCollection role, @NotNull DataCollection.Version version, String signature) {
        Table table1 = testTable();
        Table table2 = testTable();

        DataCollectionTable link1;
        DataCollectionTable link2;
        // create two links to test unique constraint
        try {
            link1 = dataCollectionEntityMgr.addTableToCollection(collectionName, table1.getName(), role, signature,
                    version);
            link2 = dataCollectionEntityMgr.addTableToCollection(collectionName, table2.getName(), role, signature,
                    version);
        } finally {
            // cleanup afterwards
            dataCollectionEntityMgr.removeTableFromCollection(collectionName, table1.getName(), version);
            dataCollectionEntityMgr.removeTableFromCollection(collectionName, table2.getName(), version);
        }
        return Pair.of(link1, link2);
    }

    private void validateDataCollectionTable(DataCollectionTable table, Long expectedPid,
            @NotNull String expectedTableName, @NotNull DataCollection.Version expectedVersion,
            @NotNull TableRoleInCollection expectedRole, String expectedSignature) {
        Assert.assertNotNull(table);
        if (expectedPid == null) {
            Assert.assertNotNull(table.getPid());
        } else {
            Assert.assertEquals(table.getPid(), expectedPid);
        }
        Assert.assertEquals(table.getVersion(), expectedVersion);
        Assert.assertEquals(table.getRole(), expectedRole);
        Assert.assertEquals(table.getSignature(), expectedSignature);
        Assert.assertNotNull(table.getTable());
        Assert.assertEquals(table.getTable().getName(), expectedTableName);
    }

    private Table testTable() {
        String tableName = NamingUtils.uuid(getClass().getSimpleName());
        Table table = new Table();
        table.setName(tableName);
        table.setDisplayName(tableName);
        table.setTableType(TableType.DATATABLE);
        tableEntityMgr.create(table);
        return table;
    }

}
