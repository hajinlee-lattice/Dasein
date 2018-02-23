package com.latticeengines.metadata.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollection.Version;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.DataFeedService;

public class DataFeedServiceImplTestNG extends MetadataFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DataFeedServiceImplTestNG.class);

    @Autowired
    private DataFeedService datafeedService;

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
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace1));
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        datafeedTaskEntityMgr.clearTableQueue();
        super.cleanup();
    }

    @Test(groups = "functional")
    public void create() {
        DataCollection dataCollection = new DataCollection();
        dataCollection.setName("DATA_COLLECTION_NAME");
        dataCollection.setTenant(MultiTenantContext.getTenant());
        dataCollection.setVersion(Version.Blue);
        dataCollectionEntityMgr.create(dataCollection);

        datafeed.setName(DATA_FEED_NAME);
        datafeed.setStatus(Status.Active);
        datafeed.setActiveExecutionId(1L);
        datafeed.setDataCollection(dataCollection);

        Table importTable = new Table(TableType.IMPORTTABLE);
        importTable.setName("importTable");
        importTable.setDisplayName(importTable.getName());
        importTable.setTenant(MultiTenantContext.getTenant());
        tableEntityMgr.create(importTable);

        Table dataTable = new Table(TableType.DATATABLE);
        dataTable.setName("dataTable");
        dataTable.setDisplayName(dataTable.getName());
        dataTable.setTenant(MultiTenantContext.getTenant());
        Extract e = new Extract();
        e.setName("extract");
        e.setPath("abc");
        e.setExtractionTimestamp(1L);
        e.setProcessedRecords(100L);
        dataTable.addExtract(e);
        tableEntityMgr.create(dataTable);

        DataFeedTask task = new DataFeedTask();
        task.setDataFeed(datafeed);
        task.setActiveJob("3");
        task.setEntity(SchemaInterpretation.Account.name());
        task.setSource("SFDC");
        task.setStatus(DataFeedTask.Status.Active);
        task.setSourceConfig("config");
        task.setImportTemplate(importTable);
        task.setImportData(dataTable);
        task.setStartTime(new Date());
        task.setLastImported(new Date());
        task.setUniqueId(UUID.randomUUID().toString());
        datafeed.addTask(task);
        datafeedService.createDataFeed(MultiTenantContext.getTenant().getId(), dataCollection.getName(), datafeed);
        datafeedTaskEntityMgr.addTableToQueue(task, dataTable);
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void startExecution() throws Exception {
        String customerSpace = MultiTenantContext.getTenant().getId();
        datafeedService.updateDataFeed(customerSpace, DATA_FEED_NAME, Status.Active.getName());
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Tenant t = MultiTenantContext.getTenant();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(50L);
                    MultiTenantContext.setTenant(t);
                    assertFalse(
                            datafeedService.lockExecution(customerSpace, DATA_FEED_NAME, DataFeedExecutionJobType.PA));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        log.info("started locking execution");
        assertTrue(datafeedService.lockExecution(customerSpace, DATA_FEED_NAME, DataFeedExecutionJobType.PA));
        log.info("already locked execution");
        assertNotNull(datafeedService.startExecution(customerSpace, DATA_FEED_NAME).getImports());
        DataFeed df = datafeedService.findDataFeedByName(customerSpace, DATA_FEED_NAME);
        assertEquals(df.getStatus(), Status.ProcessAnalyzing);

        DataFeedExecution exec1 = df.getActiveExecution();
        assertEquals(exec1.getStatus(), DataFeedExecution.Status.Started);
        assertEquals(exec1.getImports().size(), df.getTasks().size());
        executor.shutdown();
    }

    @Test(groups = "functional", dependsOnMethods = "startExecution")
    public void finishExecution() {
        DataFeedExecution exec1 = datafeedService.finishExecution(MultiTenantContext.getTenant().getId(),
                DATA_FEED_NAME, Status.Active.getName());
        assertEquals(exec1.getStatus(), DataFeedExecution.Status.Completed);

        DataFeed df = datafeedService.findDataFeedByName(MultiTenantContext.getTenant().getId(), DATA_FEED_NAME);
        assertEquals(df.getStatus(), Status.Active);

        assertEquals(exec1.getStatus(), df.getActiveExecution().getStatus());
        assertEquals(exec1.getImports().size(), df.getTasks().size());

    }
}
