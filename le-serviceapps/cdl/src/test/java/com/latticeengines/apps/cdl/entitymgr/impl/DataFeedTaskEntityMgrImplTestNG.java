package com.latticeengines.apps.cdl.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.DataFeedEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedTaskTableEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

public class DataFeedTaskEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private DataFeedEntityMgr datafeedEntityMgr;

    @Inject
    private DataFeedTaskEntityMgr datafeedTaskEntityMgr;

    @Inject
    private DataFeedTaskTableEntityMgr datafeedTaskTableEntityMgr;

    private DataFeed datafeed = new DataFeed();

    private DataFeedTask task = new DataFeedTask();

    private Table importTable = new Table();

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
    }

    @BeforeMethod(groups = "functional")
    public void beforeMethod() {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(mainCustomerSpace));
    }

    @Test(groups = "functional")
    public void create() {
        datafeed.setName("datafeed");
        datafeed.setDataCollection(dataCollection);

        importTable.setName("importTable");
        importTable.setDisplayName(importTable.getName());
        importTable.setTenant(MultiTenantContext.getTenant());
        Attribute a1 = new Attribute();
        a1.setName("a1");
        a1.setDisplayName(a1.getName());
        a1.setPhysicalDataType("string");
        importTable.addAttribute(a1);

        task.setDataFeed(datafeed);
        task.setActiveJob("Not specified");
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
        DataFeed dataFeed = datafeedEntityMgr.findByName("datafeed");
        dataFeed.setStatus(Status.Active);
        datafeedEntityMgr.update(dataFeed);
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void retrieve() throws InterruptedException {
        assertEquals(datafeedTaskTableEntityMgr.countDataFeedTaskTables(task), 0);

        Table dataTable = new Table();
        dataTable.setName("dataTable2");
        dataTable.setDisplayName(dataTable.getName());
        dataTable.setTenant(MultiTenantContext.getTenant());
        datafeedTaskEntityMgr.addTableToQueue(task, dataTable);
        int count = datafeedTaskTableEntityMgr.countDataFeedTaskTables(task);
        int retry = 0;
        while (count == 0 && retry < 5) {
            Thread.sleep(200);
            count = datafeedTaskTableEntityMgr.countDataFeedTaskTables(task);
            retry++;
        }
        assertEquals(count, 1);
        assertEquals(datafeedTaskTableEntityMgr.pollFirstDataTable(task).toString(), dataTable.toString());
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
        List<String> tables = datafeedTaskEntityMgr.registerExtract(task, task.getImportTemplate().getName(), extract1);
        task = datafeedTaskEntityMgr.findByKey(task);
        assertEquals(datafeedTaskTableEntityMgr.countDataFeedTaskTables(task), 0);
        datafeedTaskEntityMgr.addTableToQueue(task.getUniqueId(), tables.get(0));
        assertEquals(datafeedTaskTableEntityMgr.countDataFeedTaskTables(task), 1);
        assertEquals(datafeedTaskTableEntityMgr.peekFirstDataTable(task).getExtracts().get(0).getPid(),
                extract1.getPid());
        assertEquals(task.getStatus(), DataFeedTask.Status.Active);

    }

    @Test(groups = "functional", dependsOnMethods = "registerExtractWhenTemplateChanged")
    public void getExtractPendingInQueueAfterFirstRegister() {
        task = datafeedTaskEntityMgr.findByKey(task);
        List<Extract> extracts = datafeedTaskEntityMgr.getExtractsPendingInQueue(task);
        assertEquals(extracts.size(), 1);
        assertEquals(extracts.get(0).getName(), "extract1");
        assertEquals(extracts.get(0).getProcessedRecords().longValue(), 1L);
    }

    @Test(groups = "functional", dependsOnMethods = "getExtractPendingInQueueAfterFirstRegister")
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
        List<String> tables = datafeedTaskEntityMgr.registerExtract(task, task.getImportTemplate().getName(), extract2);
        assertEquals(datafeedTaskTableEntityMgr.countDataFeedTaskTables(task), 0);
        datafeedTaskEntityMgr.addTableToQueue(task.getUniqueId(), tables.get(0));
        assertEquals(datafeedTaskTableEntityMgr.countDataFeedTaskTables(task), 1);
        assertEquals(datafeedTaskTableEntityMgr.peekFirstDataTable(task).getExtracts().get(0).getPid(),
                extract2.getPid());
    }

    @Test(groups = "functional", dependsOnMethods = "registerExtractWhenDataTableConsumed")
    public void getExtractPendingInQueueAfterSecondRegister() {
        task = datafeedTaskEntityMgr.findByKey(task);
        List<Extract> extracts = datafeedTaskEntityMgr.getExtractsPendingInQueue(task);
        assertEquals(extracts.size(), 1);
        assertEquals(extracts.get(0).getName(), "extract2");
        assertEquals(extracts.get(0).getProcessedRecords().longValue(), 2L);
    }

}
