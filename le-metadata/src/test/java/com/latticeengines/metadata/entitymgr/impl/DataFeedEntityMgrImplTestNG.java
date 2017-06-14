package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class DataFeedEntityMgrImplTestNG extends MetadataFunctionalTestNGBase {

    @Autowired
    private DataFeedEntityMgr datafeedEntityMgr;

    @Autowired
    private DataFeedTaskEntityMgr datafeedTaskEntityMgr;

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    private static final String DATA_FEED_NAME = "datafeed";

    private DataFeed datafeed = new DataFeed();

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
        dataCollection.setType(DataCollectionType.Segmentation);
        dataCollectionEntityMgr.createDataCollection(dataCollection);

        datafeed.setName(DATA_FEED_NAME);
        datafeed.setStatus(Status.Active);
        datafeed.setDataCollectionType(DataCollectionType.Segmentation);
        dataCollection.addDataFeed(datafeed);

        Table importTable = new Table();
        importTable.setName("importTable");
        importTable.setDisplayName(importTable.getName());
        importTable.setTenant(MultiTenantContext.getTenant());

        Table dataTable = new Table();
        dataTable.setName("dataTable");
        dataTable.setDisplayName(dataTable.getName());
        dataTable.setTenant(MultiTenantContext.getTenant());

        DataFeedTask task = new DataFeedTask();
        task.setDataFeed(datafeed);
        task.setActiveJob("Not specified");
        task.setFeedType("VisiDB");
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
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void retrieve() {
        DataFeed retrieved = datafeedEntityMgr.findByNameInflatedWithAllExecutions(DATA_FEED_NAME);
        assertEquals(retrieved.getName(), datafeed.getName());
        assertEquals(retrieved.getActiveExecution().getPid(), datafeed.getActiveExecutionId());
        assertEquals(retrieved.getExecutions().size(), 1);
        assertEquals(retrieved.getTasks().size(), 1);
        assertEquals(retrieved.getTasks().get(0).getImportTemplate().getTableType(), TableType.IMPORTTABLE);
        assertNotNull(retrieved.getTasks().get(0).getImportTemplate().getPid());
        assertEquals(retrieved.getTasks().get(0).getImportData().getTableType(), TableType.DATATABLE);
        assertNotNull(retrieved.getTasks().get(0).getImportData().getPid());
    }

    @Test(groups = "functional", dependsOnMethods = "retrieve")
    public void startExecution() {
        assertNotNull(datafeedEntityMgr.startExecution(DATA_FEED_NAME).getImports());
        DataFeed df = datafeedEntityMgr.findByNameInflatedWithAllExecutions(DATA_FEED_NAME);
        assertEquals(df.getActiveExecution().getPid(), df.getActiveExecutionId());
        assertEquals(df.getExecutions().size(), 1);
        assertEquals(df.getStatus(), Status.Consolidating);

        DataFeedExecution exec = df.getExecutions().get(0);
        assertEquals(exec.getStatus(), DataFeedExecution.Status.Started);
        assertEquals(exec.getImports().size(), df.getTasks().size());

    }

    @Test(groups = "functional", dependsOnMethods = "startExecution")
    public void finishExecution() {
        DataFeedExecution exec1 = datafeedEntityMgr.updateExecutionWithTerminalStatus(DATA_FEED_NAME,
                DataFeedExecution.Status.Consolidated);
        assertEquals(exec1.getStatus(), DataFeedExecution.Status.Consolidated);

        DataFeed df = datafeedEntityMgr.findByNameInflatedWithAllExecutions(DATA_FEED_NAME);
        assertEquals(df.getActiveExecution().getPid(), df.getActiveExecutionId());
        assertEquals(df.getExecutions().size(), 2);
        assertEquals(df.getStatus(), Status.Active);

        assertEquals(exec1.getStatus(), df.getExecutions().get(0).getStatus());
        assertEquals(exec1.getImports().size(), df.getTasks().size());

        DataFeedExecution exec2 = df.getExecutions().get(1);
        assertEquals(exec2.getStatus(), DataFeedExecution.Status.Active);
        assertEquals(exec2.getImports().size(), 0);

    }

}
