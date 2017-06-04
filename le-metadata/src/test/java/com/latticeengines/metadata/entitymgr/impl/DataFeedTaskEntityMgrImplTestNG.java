package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.Collections;
import java.util.Date;

import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class DataFeedTaskEntityMgrImplTestNG extends MetadataFunctionalTestNGBase {

    @Autowired
    private DataFeedEntityMgr datafeedEntityMgr;

    @Autowired
    private DataFeedTaskEntityMgr datafeedTaskEntityMgr;

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    private DataFeed datafeed = new DataFeed();

    private DataFeedTask task = new DataFeedTask();

    private Table dataTable = new Table(TableType.DATATABLE);

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        datafeedTaskEntityMgr.clearTableQueue();
        super.cleanup();
    }

    @BeforeMethod(groups = "functional")
    public void beforeMethod() {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace1));
    }

    @Test(groups = "functional")
    public void create() {
        DataCollection dataCollection = new DataCollection();
        dataCollection.setName("DATA_COLLECTION_NAME");
        Table table = new Table();
        table.setName(TABLE1);
        dataCollection.setTables(Collections.singletonList(table));
        dataCollection.setType(DataCollectionType.Segmentation);
        dataCollectionEntityMgr.createDataCollection(dataCollection);

        datafeed.setName("datafeed");
        datafeed.setStatus(Status.Active);
        datafeed.setActiveExecution(1L);
        datafeed.setDataCollection(dataCollection);
        dataCollection.addDataFeed(datafeed);

        Table importTable = new Table(TableType.IMPORTTABLE);
        importTable.setName("importTable");
        importTable.setDisplayName(importTable.getName());
        importTable.setTenant(MultiTenantContext.getTenant());

        dataTable.setName("dataTable");
        dataTable.setDisplayName(dataTable.getName());
        dataTable.setTenant(MultiTenantContext.getTenant());

        task.setFeed(datafeed);
        task.setActiveJob(3L);
        task.setEntity(SchemaInterpretation.Account.name());
        task.setSource("SFDC");
        task.setStatus(DataFeedTask.Status.Active);
        task.setSourceConfig("config");
        task.setImportTemplate(importTable);
        task.setImportData(dataTable);
        task.setStartTime(new Date());
        task.setLastImported(new Date());
        datafeed.addTask(task);
        datafeedEntityMgr.create(datafeed);
        datafeedTaskEntityMgr.create(task);
        datafeedTaskEntityMgr.addImportDataTableToQueue(task);
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void retrieve() {
        assertEquals(datafeedTaskEntityMgr.getDataTableSize(task.getPid()), 1);
        assertEquals(datafeedTaskEntityMgr.peekFirstDataTable(task.getPid()).toString(), dataTable.toString());

        Table dataTable2 = new Table(TableType.DATATABLE);
        dataTable2.setName("dataTable2");
        dataTable2.setDisplayName(dataTable2.getName());
        dataTable2.setTenant(MultiTenantContext.getTenant());
        task.setImportData(dataTable2);
        datafeedTaskEntityMgr.addImportDataTableToQueue(task);

        assertEquals(datafeedTaskEntityMgr.getDataTableSize(task.getPid()), 2);
        assertEquals(datafeedTaskEntityMgr.pollFirstDataTable(task.getPid()).toString(), dataTable.toString());
        assertEquals(datafeedTaskEntityMgr.getDataTableSize(task.getPid()), 1);
        assertEquals(datafeedTaskEntityMgr.peekFirstDataTable(task.getPid()).toString(), dataTable2.toString());
    }

    @Test(groups = "functional", dependsOnMethods = "retrieve")
    public void registerExtractWhenTemplateChanged() {
        task = datafeedTaskEntityMgr.findByKey(task);
        task.setStatus(DataFeedTask.Status.Updated);
        Extract extract1 = new Extract();
        extract1.setName("extract1");
        extract1.setPath("path1");
        extract1.setExtractionTimestamp(DateTime.now().getMillis());
        extract1.setProcessedRecords(1L);
        extract1.setTable(task.getImportTemplate());
        datafeedTaskEntityMgr.registerExtract(task, extract1);
        task = datafeedTaskEntityMgr.findByKey(task);
        assertEquals(datafeedTaskEntityMgr.getDataTableSize(task.getPid()), 2);
        assertEquals(datafeedTaskEntityMgr.peekFirstDataTable(task.getPid()).getExtracts().get(0).getPid(),
                extract1.getPid());
        assertEquals(task.getImportData().getDisplayName(), task.getImportTemplate().getDisplayName());
        assertEquals(task.getStatus(), DataFeedTask.Status.Active);

    }

    @Test(groups = "functional", dependsOnMethods = "registerExtractWhenTemplateChanged")
    public void registerExtractWhenDataTableConsumed() {
        task = datafeedTaskEntityMgr.findByKey(task);
        task.setImportData(null);
        task.getImportTemplate().setDisplayName("new displayname");
        Extract extract2 = new Extract();
        extract2.setName("extract2");
        extract2.setPath("path2");
        extract2.setExtractionTimestamp(DateTime.now().getMillis());
        extract2.setProcessedRecords(2L);
        extract2.setTable(task.getImportTemplate());
        datafeedTaskEntityMgr.clearTableQueue();
        datafeedTaskEntityMgr.registerExtract(task, extract2);
        task = datafeedTaskEntityMgr.findByKey(task);
        assertEquals(datafeedTaskEntityMgr.getDataTableSize(task.getPid()), 2);
        assertEquals(datafeedTaskEntityMgr.peekFirstDataTable(task.getPid()).getPid(),
                new Long(task.getImportData().getPid() - 1));
        assertEquals(datafeedTaskEntityMgr.peekFirstDataTable(task.getPid()).getExtracts().get(0).getPid(),
                extract2.getPid());
        assertEquals(task.getImportData().getDisplayName(), task.getImportTemplate().getDisplayName());
    }
}
