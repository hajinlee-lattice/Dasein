package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.Collections;
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
import com.latticeengines.metadata.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class DataFeedEntityMgrImplTestNG extends MetadataFunctionalTestNGBase {

    @Autowired
    private DataFeedEntityMgr datafeedEntityMgr;

    @Autowired
    private DataFeedExecutionEntityMgr datafeedExecutionEntityMgr;

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
        datafeedEntityMgr.delete(datafeed);
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

        datafeed.setName(DATA_FEED_NAME);
        datafeed.setStatus(Status.Active);
        datafeed.setActiveExecution(1L);
        datafeed.setDataCollection(dataCollection);
        dataCollection.addDataFeed(datafeed);

        DataFeedExecution execution = new DataFeedExecution();
        execution.setFeed(datafeed);
        execution.setStatus(DataFeedExecution.Status.Active);
        datafeed.addExeuction(execution);

        Table importTable = new Table(TableType.IMPORTTABLE);
        importTable.setName("importTable");
        importTable.setDisplayName(importTable.getName());
        importTable.setTenant(MultiTenantContext.getTenant());

        Table dataTable = new Table(TableType.DATATABLE);
        dataTable.setName("dataTable");
        dataTable.setDisplayName(dataTable.getName());
        dataTable.setTenant(MultiTenantContext.getTenant());

        DataFeedTask task = new DataFeedTask();
        task.setFeed(datafeed);
        task.setActiveJob(3L);
        task.setEntity(SchemaInterpretation.Account.name());
        task.setSource("SFDC");
        task.setStatus(DataFeedTask.Status.Active);
        task.setStagingDir("/tmp");
        task.setSourceConfig("config");
        task.setImportTemplate(importTable);
        task.setImportData(dataTable);
        task.setStartTime(new Date());
        task.setLastImported(new Date());
        datafeed.addTask(task);
        datafeedEntityMgr.create(datafeed);
        datafeedTaskEntityMgr.create(task);
        datafeedExecutionEntityMgr.create(execution);
        datafeed.setActiveExecution(execution.getPid());
        datafeedEntityMgr.update(datafeed);
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void retrieve() {
        DataFeed retrieved = datafeedEntityMgr.findByName(DATA_FEED_NAME);
        assertEquals(retrieved.getName(), datafeed.getName());
        assertEquals(retrieved.getActiveExecution(), datafeed.getActiveExecution());
        assertEquals(retrieved.getExecutions().size(), 1);
        assertEquals(retrieved.getTasks().size(), 1);
    }

    @Test(groups = "functional", dependsOnMethods = "retrieve")
    public void startExecution() {
        datafeedEntityMgr.startExecution(DATA_FEED_NAME);
        DataFeed df = datafeedEntityMgr.findByName(DATA_FEED_NAME);
        assertEquals(df.getActiveExecution(), new Long(datafeed.getActiveExecution() + 1L));
        assertEquals(df.getExecutions().size(), 2);

        DataFeedExecution exec1 = df.getExecutions().get(0);
        assertEquals(exec1.getStatus(), DataFeedExecution.Status.Started);
        assertEquals(exec1.getImports().size(), df.getTasks().size());

        DataFeedExecution exec2 = df.getExecutions().get(1);
        assertEquals(exec2.getStatus(), DataFeedExecution.Status.Active);
        assertEquals(exec2.getImports().size(), 0);
    }

}
