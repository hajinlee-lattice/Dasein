package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedProfile;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.metadata.entitymgr.DataFeedEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.metadata.functionalframework.DataCollectionFunctionalTestNGBase;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class DataFeedEntityMgrImplTestNG extends DataCollectionFunctionalTestNGBase {

    @Autowired
    private DataFeedEntityMgr datafeedEntityMgr;

    @Autowired
    private DataFeedTaskEntityMgr datafeedTaskEntityMgr;

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
        datafeed.setDataCollection(dataCollection);
        datafeed.setName(DATA_FEED_NAME);

        Table importTable = new Table();
        importTable.setName("importTable");
        importTable.setDisplayName(importTable.getName());
        importTable.setTenant(MultiTenantContext.getTenant());
        Attribute a1 = new Attribute();
        a1.setName("a1");
        a1.setDisplayName(a1.getName());
        a1.setPhysicalDataType("string");
        importTable.addAttribute(a1);
        importTable.setTableType(TableType.IMPORTTABLE);

        Table dataTable = new Table();
        dataTable.setName("dataTable");
        dataTable.setDisplayName(dataTable.getName());
        dataTable.setTenant(MultiTenantContext.getTenant());
        Attribute a2 = new Attribute();
        a2.setName("a2");
        a2.setDisplayName(a1.getName());
        a2.setPhysicalDataType("string");
        dataTable.addAttribute(a2);
        dataTable.setTableType(TableType.DATATABLE);

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
        task.setUniqueId(NamingUtils.uuid("DataFeedTask"));
        datafeed.addTask(task);

        datafeedEntityMgr.create(datafeed);
        DataFeed dataFeed = datafeedEntityMgr.findByName(DATA_FEED_NAME);
        dataFeed.setStatus(Status.Active);
        datafeedEntityMgr.update(dataFeed);
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void retrieve() {
        DataFeed retrieved = datafeedEntityMgr.findByNameInflatedWithAllExecutions(DATA_FEED_NAME);
        assertEquals(retrieved.getName(), datafeed.getName());
        assertNull(retrieved.getActiveExecutionId());
        assertNull(retrieved.getActiveExecution());
        assertEquals(retrieved.getExecutions().size(), 0);
        assertEquals(retrieved.getTasks().size(), 1);
        assertEquals(retrieved.getTasks().get(0).getImportTemplate().getTableType(), TableType.IMPORTTABLE);
        assertNotNull(retrieved.getTasks().get(0).getImportTemplate().getPid());
        assertEquals(retrieved.getTasks().get(0).getImportData().getTableType(), TableType.DATATABLE);
        assertNotNull(retrieved.getTasks().get(0).getImportData().getPid());
        JsonUtils.deserialize(JsonUtils.serialize(retrieved), DataFeed.class);
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
        assertEquals(exec.getImports().get(0).getDataTable().getAttributes().size(), 1);
    }

    @Test(groups = "functional", dependsOnMethods = "startExecution")
    public void finishExecution() {
        DataFeedExecution exec1 = datafeedEntityMgr.updateExecutionWithTerminalStatus(DATA_FEED_NAME,
                DataFeedExecution.Status.Consolidated, Status.InitialConsolidated);
        assertEquals(exec1.getStatus(), DataFeedExecution.Status.Consolidated);

        DataFeed df = datafeedEntityMgr.findByNameInflatedWithAllExecutions(DATA_FEED_NAME);
        assertEquals(df.getActiveExecution().getPid(), df.getActiveExecutionId());
        assertEquals(df.getExecutions().size(), 1);
        assertEquals(df.getStatus(), Status.InitialConsolidated);

        assertEquals(exec1.getStatus(), df.getExecutions().get(0).getStatus());
        assertEquals(exec1.getImports().size(), df.getTasks().size());

    }

    @Test(groups = "functional", dependsOnMethods = "finishExecution")
    public void retryLatestExecution() {
        DataFeedExecution exec1 = datafeedEntityMgr.updateExecutionWithTerminalStatus(DATA_FEED_NAME,
                DataFeedExecution.Status.Failed, Status.InitialLoaded);
        assertEquals(exec1.getStatus(), DataFeedExecution.Status.Failed);

        datafeedEntityMgr.retryLatestExecution(DATA_FEED_NAME);

        DataFeed df = datafeedEntityMgr.findByNameInflatedWithAllExecutions(DATA_FEED_NAME);
        assertEquals(df.getActiveExecution().getPid(), df.getActiveExecutionId());
        assertEquals(df.getExecutions().size(), 1);
        assertEquals(df.getStatus(), Status.Consolidating);

        DataFeedExecution exec = df.getExecutions().get(0);
        assertEquals(exec.getStatus(), DataFeedExecution.Status.Started);
        assertEquals(exec.getImports().size(), exec1.getImports().size());
        assertEquals(exec.getImports().get(0).getDataTable().getAttributes().size(), 1);
    }

    @Test(groups = "functional", dependsOnMethods = "retryLatestExecution")
    public void startProfile() {
        DataFeedProfile profile = datafeedEntityMgr.startProfile(DATA_FEED_NAME);
        DataFeed df = datafeedEntityMgr.findByNameInflatedWithAllExecutions(DATA_FEED_NAME);
        assertEquals(df.getStatus(), Status.Profiling);
        assertEquals(df.getActiveProfileId(), profile.getPid());
        assertEquals(df.getActiveExecutionId(), profile.getLatestDataFeedExecutionId());
    }

}
