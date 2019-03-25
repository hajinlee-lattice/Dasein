package com.latticeengines.apps.cdl.entitymgr.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Date;
import java.util.List;

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
import com.latticeengines.domain.exposed.metadata.datafeed.SimpleDataFeed;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.security.exposed.service.TenantService;

public class DataFeedEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private DataFeedEntityMgr datafeedEntityMgr;

    @Inject
    private DataFeedExecutionEntityMgr datafeedExecutionEntityMgr;

    @Inject
    private DataFeedTaskEntityMgr datafeedTaskEntityMgr;

    @Inject
    private TenantService tenantService;

    private static final String DATA_FEED_NAME = "datafeed";

    private String uniqueDataFeedName;

    private DataFeed datafeed = new DataFeed();

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
        uniqueDataFeedName = NamingUtils.timestamp(DATA_FEED_NAME);
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
        datafeed.setName(uniqueDataFeedName);

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
        task.setLastUpdated(new Date());
        task.setUniqueId(NamingUtils.uuid("DataFeedTask"));
        datafeed.addTask(task);

        datafeedEntityMgr.create(datafeed);
        DataFeed dataFeed = datafeedEntityMgr.findByName(uniqueDataFeedName);
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
    public void getAllSimpleDataFeedsForActiveTenant() {
        mainTestTenant.setStatus(TenantStatus.INACTIVE);
        tenantService.updateTenant(mainTestTenant);

        List<SimpleDataFeed> simpleDataFeeds = datafeedEntityMgr.getSimpleDataFeeds(TenantStatus.ACTIVE, "4.0");
        int size_1 = simpleDataFeeds != null ? simpleDataFeeds.size() : 0;

        mainTestTenant.setStatus(TenantStatus.ACTIVE);
        tenantService.updateTenant(mainTestTenant);

        simpleDataFeeds = datafeedEntityMgr.getSimpleDataFeeds(TenantStatus.ACTIVE, "4.0");
        if (simpleDataFeeds != null) {
            int size_2 = simpleDataFeeds != null ? simpleDataFeeds.size() : 0;
            assertTrue(size_2 > size_1);
        }
    }

    @Test(groups = "functional", dependsOnMethods = "getAllSimpleDataFeedsForActiveTenant")
    public void updateStatus() {
        assertEquals(DataFeed.Status.ProcessAnalyzing, datafeed.getStatus());
        datafeed.setStatus(DataFeed.Status.Active);
        datafeed = datafeedEntityMgr.updateStatus(datafeed);
        assertEquals(DataFeed.Status.Active, datafeed.getStatus());
        datafeed.setStatus(DataFeed.Status.ProcessAnalyzing);
        datafeed = datafeedEntityMgr.updateStatus(datafeed);
        assertEquals(DataFeed.Status.ProcessAnalyzing, datafeed.getStatus());
    }

    @Test(groups = "functional", dependsOnMethods = "updateStatus")
    public void retrieve() {
        DataFeed retrieved = datafeedEntityMgr.findByNameInflated(uniqueDataFeedName);
        assertEquals(retrieved.getName(), datafeed.getName());
        assertNotNull(retrieved.getActiveExecutionId());
        assertNotNull(retrieved.getActiveExecution());
        assertNotNull(retrieved.getActiveExecution().getCreated());
        assertEquals(retrieved.getTasks().size(), 1);
        assertEquals(retrieved.getTasks().get(0).getImportTemplate().getTableType(), TableType.IMPORTTABLE);
        assertNotNull(retrieved.getTasks().get(0).getImportTemplate().getPid());
        JsonUtils.deserialize(JsonUtils.serialize(retrieved), DataFeed.class);
    }

    @Test(groups = "functional", dependsOnMethods = "retrieve")
    public void finishExecution() throws InterruptedException {
        Thread.sleep(1000L);
        DataFeedExecution exec1 = datafeedEntityMgr.updateExecutionWithTerminalStatus(uniqueDataFeedName,
                DataFeedExecution.Status.Completed, DataFeed.Status.Active);
        assertEquals(exec1.getStatus(), DataFeedExecution.Status.Completed);
        DataFeed df = datafeedEntityMgr.findByNameInflatedWithAllExecutions(uniqueDataFeedName);
        assertEquals(df.getActiveExecution().getPid(), df.getActiveExecutionId());
        assertTrue(df.getActiveExecution().getUpdated().after(df.getActiveExecution().getCreated()));
        assertEquals(df.getExecutions().size(), 1);
        assertEquals(df.getStatus(), DataFeed.Status.Active);
        assertEquals(exec1.getStatus(), df.getExecutions().get(0).getStatus());
    }

}
