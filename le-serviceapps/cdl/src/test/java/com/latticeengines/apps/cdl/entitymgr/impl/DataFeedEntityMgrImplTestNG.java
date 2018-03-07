package com.latticeengines.apps.cdl.entitymgr.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.Date;

import javax.inject.Inject;

import org.joda.time.DateTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.DataFeedEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

public class DataFeedEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private DataFeedEntityMgr datafeedEntityMgr;

    @Inject
    private DataFeedExecutionEntityMgr datafeedExecutionEntityMgr;

    @Inject
    private DataFeedTaskEntityMgr datafeedTaskEntityMgr;

    private static final String DATA_FEED_NAME = "datafeed";

    private DataFeed datafeed = new DataFeed();

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        datafeedTaskEntityMgr.clearTableQueue();
    }

    @BeforeMethod(groups = "functional")
    public void beforeMethod() {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(mainCustomerSpace));
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

        DataFeedTask task = new DataFeedTask();
        task.setDataFeed(datafeed);
        task.setActiveJob("Not specified");
        task.setFeedType("VisiDB");
        task.setEntity(SchemaInterpretation.Account.name());
        task.setSource("SFDC");
        task.setStatus(DataFeedTask.Status.Active);
        task.setSourceConfig("config");
        task.setImportTemplate(importTable);
        task.setStartTime(new Date());
        task.setLastImported(new Date());
        task.setUniqueId(NamingUtils.uuid("DataFeedTask"));
        datafeed.addTask(task);

        datafeedEntityMgr.create(datafeed);
        DataFeed dataFeed = datafeedEntityMgr.findByName(DATA_FEED_NAME);
        dataFeed.setStatus(DataFeed.Status.Active);
        datafeedEntityMgr.update(dataFeed);

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
        Extract extract1 = new Extract();
        extract1.setName("extract1");
        extract1.setPath("path1");
        extract1.setExtractionTimestamp(DateTime.now().getMillis());
        extract1.setProcessedRecords(1L);
        dataTable.addExtract(extract1);
        datafeedTaskEntityMgr.addTableToQueue(task, dataTable);

        DataFeedExecution execution = new DataFeedExecution();
        execution.setDataFeed(datafeed);
        execution.setStatus(DataFeedExecution.Status.Started);
        execution.setWorkflowId(1L);
        execution.setDataFeedExecutionJobType(DataFeedExecutionJobType.PA);
        datafeedExecutionEntityMgr.create(execution);

        datafeed.setActiveExecutionId(execution.getPid());
        datafeed.setStatus(DataFeed.Status.ProcessAnalyzing);
        datafeedEntityMgr.update(datafeed);
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void retrieve() {
        DataFeed retrieved = datafeedEntityMgr.findByNameInflated(DATA_FEED_NAME);
        assertEquals(retrieved.getName(), datafeed.getName());
        assertNotNull(retrieved.getActiveExecutionId());
        assertNotNull(retrieved.getActiveExecution());
        assertEquals(retrieved.getTasks().size(), 1);
        assertEquals(retrieved.getTasks().get(0).getImportTemplate().getTableType(), TableType.IMPORTTABLE);
        assertNotNull(retrieved.getTasks().get(0).getImportTemplate().getPid());
        JsonUtils.deserialize(JsonUtils.serialize(retrieved), DataFeed.class);
    }

    @Test(groups = "functional", dependsOnMethods = "retrieve")
    public void finishExecution() {
        DataFeedExecution exec1 = datafeedEntityMgr.updateExecutionWithTerminalStatus(DATA_FEED_NAME,
                DataFeedExecution.Status.Completed, DataFeed.Status.Active);
        assertEquals(exec1.getStatus(), DataFeedExecution.Status.Completed);

        DataFeed df = datafeedEntityMgr.findByNameInflatedWithAllExecutions(DATA_FEED_NAME);
        assertEquals(df.getActiveExecution().getPid(), df.getActiveExecutionId());
        assertEquals(df.getExecutions().size(), 1);
        assertEquals(df.getStatus(), DataFeed.Status.Active);
        assertEquals(exec1.getStatus(), df.getExecutions().get(0).getStatus());
    }

}
