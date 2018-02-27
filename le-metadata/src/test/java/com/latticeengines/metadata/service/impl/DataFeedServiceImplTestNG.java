package com.latticeengines.metadata.service.impl;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.DataCollectionService;
import com.latticeengines.metadata.service.DataFeedService;
import com.latticeengines.metadata.service.DataFeedTaskService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class DataFeedServiceImplTestNG extends MetadataFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DataFeedServiceImplTestNG.class);

    private DataFeedService datafeedService;

    @Inject
    private DataFeedEntityMgr datafeedEntityMgr;

    @Inject
    private DataFeedExecutionEntityMgr datafeedExecutionEntityMgr;

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private DataFeedTaskEntityMgr datafeedTaskEntityMgr;

    @Inject
    private DataFeedTaskService datafeedTaskService;

    @Inject
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    private static final String DATA_FEED_NAME = "datafeed";

    private DataFeed datafeed = new DataFeed();

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace1));
        Job job = new Job();
        job.setJobStatus(JobStatus.FAILED);
        job.setInputs(new HashMap<>());
        job.getInputs().put(WorkflowContextConstants.Inputs.INITIAL_DATAFEED_STATUS, Status.Active.getName());
        WorkflowProxy workflowProxy = mock(WorkflowProxy.class);
        when(workflowProxy.getWorkflowExecution(anyString(), anyString())).thenReturn(job);
        datafeedService = new DataFeedServiceImpl(datafeedEntityMgr, datafeedExecutionEntityMgr, datafeedTaskEntityMgr,
                dataCollectionService, datafeedTaskService, workflowProxy);
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
        datafeed.setStatus(Status.ProcessAnalyzing);
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

        DataFeedExecution execution = new DataFeedExecution();
        execution.setDataFeed(datafeed);
        execution.setStatus(DataFeedExecution.Status.Started);
        execution.setWorkflowId(1L);
        execution.setDataFeedExecutionJobType(DataFeedExecutionJobType.PA);
        datafeedExecutionEntityMgr.create(execution);

        datafeed.setActiveExecutionId(execution.getPid());
        datafeed.setStatus(Status.ProcessAnalyzing);
        datafeedEntityMgr.update(datafeed);
    }

    @Test(groups = "functional", dependsOnMethods = "create")
    public void startExecution() throws Exception {
        String customerSpace = MultiTenantContext.getTenant().getId();
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Tenant t = MultiTenantContext.getTenant();
        executor.execute(() -> {
            try {
                Thread.sleep(100L);
                MultiTenantContext.setTenant(t);
                log.info("thread started locking execution");
                assertNull(datafeedService.lockExecution(customerSpace, DATA_FEED_NAME, DataFeedExecutionJobType.PA));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        log.info("started locking execution");
        assertNotNull(datafeedService.startExecution(customerSpace, DATA_FEED_NAME, DataFeedExecutionJobType.PA)
                .getImports());
        log.info("already locked execution");
        DataFeed df = datafeedService.findDataFeedByName(customerSpace, DATA_FEED_NAME);
        assertEquals(df.getStatus(), Status.ProcessAnalyzing);

        List<DataFeedExecution> execs = datafeedExecutionEntityMgr.findByDataFeed(df);
        assertEquals(execs.size(), 2);
        DataFeedExecution exec0 = execs.get(0);
        assertEquals(exec0.getStatus(), DataFeedExecution.Status.Failed);

        DataFeedExecution exec1 = execs.get(1);
        assertEquals(exec1.getPid(), df.getActiveExecution().getPid());
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

    @Test(groups = "functional", dependsOnMethods = "finishExecution", enabled = true)
    public void restartExecution() {
        DataFeed df = datafeedService.findDataFeedByName(MultiTenantContext.getTenant().getId(), DATA_FEED_NAME);
        DataFeedExecution exec0 = df.getActiveExecution();
        assertEquals(exec0.getStatus(), DataFeedExecution.Status.Completed);

        exec0.setWorkflowId(2L);
        exec0.setStatus(DataFeedExecution.Status.Started);
        datafeedExecutionEntityMgr.update(exec0);
        df.setStatus(Status.ProcessAnalyzing);
        datafeedEntityMgr.update(df);

        DataFeedExecution exec1 = datafeedService.restartExecution(MultiTenantContext.getTenant().getId(),
                DATA_FEED_NAME, DataFeedExecutionJobType.PA);
        assertEquals(exec1.getStatus(), DataFeedExecution.Status.Started);

        exec0 = datafeedExecutionEntityMgr.findByPid(exec0.getPid());
        assertEquals(exec0.getStatus(), DataFeedExecution.Status.Failed);
        assertEquals(exec0.getImports().size(), exec1.getImports().size());

        df = datafeedService.findDataFeedByName(MultiTenantContext.getTenant().getId(), DATA_FEED_NAME);
        assertEquals(df.getStatus(), Status.ProcessAnalyzing);
        assertEquals(exec1.getPid(), df.getActiveExecution().getPid());
        assertEquals(exec1.getStatus(), df.getActiveExecution().getStatus());

    }
}
