package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.Date;
import java.util.List;

import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.metadata.entitymgr.DataFeedEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.metadata.functionalframework.DataCollectionFunctionalTestNGBase;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class DataFeedTaskEntityMgrImplTestNG extends DataCollectionFunctionalTestNGBase {

    @Autowired
    private DataFeedEntityMgr datafeedEntityMgr;

    @Autowired
    private DataFeedTaskEntityMgr datafeedTaskEntityMgr;

    private DataFeed datafeed = new DataFeed();

    private DataFeedTask task = new DataFeedTask();

    private Table dataTable = new Table();
    private Table importTable = new Table();

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

        dataTable.setName("dataTable");
        dataTable.setDisplayName(dataTable.getName());
        dataTable.setTenant(MultiTenantContext.getTenant());

        task.setDataFeed(datafeed);
        task.setActiveJob("Not specified");
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
        DataFeed dataFeed = datafeedEntityMgr.findByName("datafeed");
        dataFeed.setStatus(Status.Active);
        datafeedEntityMgr.update(dataFeed);
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void retrieve() {
        assertEquals(datafeedTaskEntityMgr.getDataTableSize(task), 1);
        assertEquals(datafeedTaskEntityMgr.peekFirstDataTable(task).toString(), dataTable.toString());

        Table dataTable2 = new Table();
        dataTable2.setName("dataTable2");
        dataTable2.setDisplayName(dataTable2.getName());
        dataTable2.setTenant(MultiTenantContext.getTenant());
        task.setImportData(dataTable2);
        datafeedTaskEntityMgr.addImportDataTableToQueue(task);

        assertEquals(datafeedTaskEntityMgr.getDataTableSize(task), 2);
        assertEquals(datafeedTaskEntityMgr.pollFirstDataTable(task).toString(), dataTable.toString());
        assertEquals(datafeedTaskEntityMgr.getDataTableSize(task), 1);
        assertEquals(datafeedTaskEntityMgr.peekFirstDataTable(task).toString(), dataTable2.toString());
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
        datafeedTaskEntityMgr.registerExtract(task, task.getImportTemplate().getName(), extract1);
        task = datafeedTaskEntityMgr.findByKey(task);
        assertEquals(datafeedTaskEntityMgr.getDataTableSize(task), 2);
        assertEquals(datafeedTaskEntityMgr.peekFirstDataTable(task).getExtracts().get(0).getPid(), extract1.getPid());
        assertEquals(task.getImportData().getDisplayName(), task.getImportTemplate().getDisplayName());
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
        datafeedTaskEntityMgr.registerExtract(task, task.getImportTemplate().getName(), extract2);
        assertEquals(datafeedTaskEntityMgr.getDataTableSize(task), 2);
        assertEquals(datafeedTaskEntityMgr.peekFirstDataTable(task).getPid(),
                new Long(task.getImportData().getPid() - 1));
        assertEquals(datafeedTaskEntityMgr.peekFirstDataTable(task).getExtracts().get(0).getPid(), extract2.getPid());
        assertEquals(task.getImportData().getDisplayName(), task.getImportTemplate().getDisplayName());
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
