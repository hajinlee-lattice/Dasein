package com.latticeengines.metadata.entitymgr.impl;

import static com.latticeengines.domain.exposed.metadata.MetadataConstants.DATE_FORMAT;
import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionProperty;
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
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.SegmentService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class DataCollectionEntityMgrImplTestNG extends MetadataFunctionalTestNGBase {
    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Autowired
    private TableEntityMgr tableEntityMgr;

    @Autowired
    private StatisticsContainerEntityMgr statisticsContainerEntityMgr;

    @Autowired
    private SegmentService segmentService;

    private static final DataCollection dataCollection = new DataCollection();
    private static final String DATA_COLLECTION_NAME = "TestDataCollection";

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

    @BeforeMethod(groups = "functional")
    public void beforeMethod() {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace1));
    }

    @Test(groups = "functional")
    public void create() {
        dataCollection.setName(DATA_COLLECTION_NAME);
        Table table1 = new Table();
        table1.setName(tableName1);
        table1.setDisplayName(tableName1);
        table1.setTableType(TableType.DATATABLE);
        tableEntityMgr.create(table1);

        Table table2 = new Table();
        table2.setName(tableName2);
        table2.setDisplayName(tableName2);
        table2.setTableType(TableType.DATATABLE);
        tableEntityMgr.create(table2);

        dataCollection.setTables(Arrays.asList(table1, table2));
        dataCollection.setType(DataCollectionType.Segmentation);

        List<DataCollectionProperty> properties = new ArrayList<>();
        properties.add(new DataCollectionProperty("key1", "value1"));
        properties.add(new DataCollectionProperty("key2", "value2"));
        dataCollection.setProperties(properties);

        dataCollectionEntityMgr.createDataCollection(dataCollection);
        dataCollectionEntityMgr.upsertTableToCollection(dataCollection.getName(), tableName1, TableRoleInCollection.ConsolidatedAccount);
        dataCollectionEntityMgr.upsertTableToCollection(dataCollection.getName(), tableName2, TableRoleInCollection.Profile);

        DataCollection collection = dataCollectionEntityMgr.getDataCollection(dataCollection.getName());
        Assert.assertEquals(collection.getName(), DATA_COLLECTION_NAME);
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void findTableByRole() {
        List<Table> tables = dataCollectionEntityMgr.getTablesOfRole(dataCollection.getName(), TableRoleInCollection.ConsolidatedAccount);
        Assert.assertNotNull(tables);
        Assert.assertEquals(tables.size(), 1);
        Assert.assertEquals(tables.get(0).getName(), tableName1);

        tables = dataCollectionEntityMgr.getTablesOfRole(dataCollection.getName(), TableRoleInCollection.Profile);
        Assert.assertNotNull(tables);
        Assert.assertEquals(tables.size(), 1);
        Assert.assertEquals(tables.get(0).getName(), tableName2);
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void checkMasterSegment() {
        MetadataSegment masterSegment = segmentService.findMaster(customerSpace1, dataCollection.getName());
        Assert.assertNotNull(masterSegment);

        Statistics statistics = new Statistics();
        StatisticsContainer statisticsContainer = new StatisticsContainer();
        statisticsContainer.setStatistics(statistics);
        dataCollectionEntityMgr.upsertStatsForMasterSegment(dataCollection.getName(), statisticsContainer, null);

        StatisticsContainer retrieved = statisticsContainerEntityMgr.findInMasterSegment(dataCollection.getName(), null);
        Assert.assertNotNull(retrieved);
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void retrieve() {
        DataCollection retrieved = dataCollectionEntityMgr.getDataCollection(DATA_COLLECTION_NAME);
        assertEquals(retrieved.getName(), dataCollection.getName());
        assertEquals(retrieved.getType(), dataCollection.getType());
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void recreate() {
        DataCollection retrieved = dataCollectionEntityMgr.getDataCollection(DATA_COLLECTION_NAME);
        String newCollectionName = "DataCollection_" + DATE_FORMAT.format(new Date());
        retrieved.setName(newCollectionName);
        dataCollectionEntityMgr.createOrUpdate(retrieved);
    }

}
