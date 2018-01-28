package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.StatisticsContainerEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.functionalframework.DataCollectionFunctionalTestNGBase;
import com.latticeengines.metadata.service.SegmentService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class DataCollectionEntityMgrImplTestNG extends DataCollectionFunctionalTestNGBase {

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

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace1));
        tableEntityMgr.deleteByName(tableName1);
        tableEntityMgr.deleteByName(tableName2);
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        super.cleanup();
    }

    @BeforeMethod(groups = "functional")
    public void beforeMethod() {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace1));
    }

    @Test(groups = "functional")
    public void create() {
        DataCollection collection = dataCollectionEntityMgr.getDataCollection(collectionName);
        Assert.assertEquals(collection.getName(), collectionName);
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void findTableByRole() {
        DataCollection.Version version = dataCollectionEntityMgr.getActiveVersion();
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

        List<Table> tables = dataCollectionEntityMgr.getTablesOfRole(collectionName, TableRoleInCollection.ConsolidatedAccount, version);
        Assert.assertNotNull(tables);
        Assert.assertEquals(tables.size(), 1);
        Assert.assertEquals(tables.get(0).getName(), tableName1);

        tables = dataCollectionEntityMgr.getTablesOfRole(collectionName, TableRoleInCollection.Profile, version);
        Assert.assertNotNull(tables);
        Assert.assertEquals(tables.size(), 1);
        Assert.assertEquals(tables.get(0).getName(), tableName2);
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void inactiveVersion() {
        DataCollection.Version inactiveVersion = dataCollectionEntityMgr.getInactiveVersion();
        Table table3 = new Table();
        table3.setName(tableName3);
        table3.setDisplayName(tableName3);
        table3.setTableType(TableType.DATATABLE);
        tableEntityMgr.create(table3);
        dataCollectionEntityMgr.upsertTableToCollection(collectionName, tableName3, TableRoleInCollection.ConsolidatedContact, inactiveVersion);

        DataCollection.Version version = dataCollectionEntityMgr.getActiveVersion();
        List<Table> tables = dataCollectionEntityMgr.getTablesOfRole(collectionName, TableRoleInCollection.ConsolidatedContact, version);
        Assert.assertNotNull(tables);
        Assert.assertTrue(tables.isEmpty());

        tables = dataCollectionEntityMgr.getTablesOfRole(collectionName, TableRoleInCollection.ConsolidatedContact, inactiveVersion);
        Assert.assertNotNull(tables);
        Assert.assertEquals(tables.size(), 1);
        Assert.assertEquals(tables.get(0).getName(), tableName3);
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void checkMasterSegment() {
        DataCollection.Version version = dataCollectionEntityMgr.getActiveVersion();
        MetadataSegment masterSegment = segmentService.findMaster(customerSpace1, collectionName);
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

}
