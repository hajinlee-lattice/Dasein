package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
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
        Table table1 = new Table();
        table1.setName(tableName1);
        table1.setDisplayName(tableName1);
        table1.setTableType(TableType.DATATABLE);
        tableEntityMgr.create(table1);
        dataCollectionEntityMgr.upsertTableToCollection(collectionName, tableName1, TableRoleInCollection.ConsolidatedAccount);

        Table table2 = new Table();
        table2.setName(tableName2);
        table2.setDisplayName(tableName2);
        table2.setTableType(TableType.DATATABLE);
        tableEntityMgr.create(table2);
        dataCollectionEntityMgr.upsertTableToCollection(collectionName, tableName2, TableRoleInCollection.Profile);

        List<Table> tables = dataCollectionEntityMgr.getTablesOfRole(collectionName, TableRoleInCollection.ConsolidatedAccount);
        Assert.assertNotNull(tables);
        Assert.assertEquals(tables.size(), 1);
        Assert.assertEquals(tables.get(0).getName(), tableName1);

        tables = dataCollectionEntityMgr.getTablesOfRole(collectionName, TableRoleInCollection.Profile);
        Assert.assertNotNull(tables);
        Assert.assertEquals(tables.size(), 1);
        Assert.assertEquals(tables.get(0).getName(), tableName2);
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void checkMasterSegment() {
        MetadataSegment masterSegment = segmentService.findMaster(customerSpace1, collectionName);
        Assert.assertNotNull(masterSegment);

        Statistics statistics = new Statistics();
        StatisticsContainer statisticsContainer = new StatisticsContainer();
        statisticsContainer.setStatistics(statistics);
        dataCollectionEntityMgr.upsertStatsForMasterSegment(collectionName, statisticsContainer);

        StatisticsContainer retrieved = statisticsContainerEntityMgr.findInMasterSegment(collectionName);
        Assert.assertNotNull(retrieved);
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void retrieve() {
        DataCollection retrieved = dataCollectionEntityMgr.getDataCollection(collectionName);
        assertEquals(retrieved.getName(), collectionName);
        assertEquals(retrieved.getType(), DataCollectionType.Segmentation);
    }

    @Test(groups = "functional", dependsOnMethods = "retrieve")
    public void recreate() {
        DataCollection retrieved = dataCollectionEntityMgr.getDataCollection(collectionName);
        String newCollectionName = NamingUtils.timestamp("DataCollection");
        retrieved.setName(newCollectionName);
        dataCollectionEntityMgr.createOrUpdate(retrieved);

        retrieved.setName(collectionName);
        dataCollectionEntityMgr.createOrUpdate(retrieved);
    }

}
