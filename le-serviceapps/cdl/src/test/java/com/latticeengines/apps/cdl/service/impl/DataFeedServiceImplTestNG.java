package com.latticeengines.apps.cdl.service.impl;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

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

import com.latticeengines.apps.cdl.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
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
import com.latticeengines.domain.exposed.metadata.datafeed.SchedulingGroup;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class DataFeedServiceImplTestNG extends CDLFunctionalTestNGBase {

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

    private String dataFeedName;

    private DataFeed datafeed = new DataFeed();

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
        dataFeedName = NamingUtils.timestamp("datafeed");
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
    }

    @Test(groups = "functional")
    public void create() {
        DataCollection dataCollection = new DataCollection();
        dataCollection.setName(NamingUtils.timestamp("DATA_COLLECTION_NAME"));
        dataCollection.setTenant(MultiTenantContext.getTenant());
        dataCollection.setVersion(Version.Blue);
        dataCollectionEntityMgr.create(dataCollection);

        datafeed.setName(dataFeedName);
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
        task.setLastUpdated(new Date());
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
    public void startExecution() {
        String customerSpace = MultiTenantContext.getTenant().getId();
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Tenant t = MultiTenantContext.getTenant();
        executor.execute(() -> {
            try {
                Thread.sleep(100L);
                MultiTenantContext.setTenant(t);
                log.info("thread started locking execution");
                assertFalse(datafeedService.lockExecution(customerSpace, dataFeedName, DataFeedExecutionJobType.PA) == -1L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        log.info("started locking execution");
        datafeedService.lockExecution(customerSpace, dataFeedName, DataFeedExecutionJobType.PA);
        log.info("already locked execution");
        assertNotNull(datafeedService.startExecution(customerSpace, dataFeedName, DataFeedExecutionJobType.PA, 2L)
                .getImports());
        DataFeed df = datafeedService.findDataFeedByName(customerSpace, dataFeedName);
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
                dataFeedName, Status.Active.getName());
        assertEquals(exec1.getStatus(), DataFeedExecution.Status.Completed);

        DataFeed df = datafeedService.findDataFeedByName(MultiTenantContext.getTenant().getId(), dataFeedName);
        assertEquals(df.getStatus(), Status.Active);

        assertEquals(exec1.getStatus(), df.getActiveExecution().getStatus());
        assertEquals(exec1.getImports().size(), df.getTasks().size());

    }

    @Test(groups = "functional", dependsOnMethods = "finishExecution", enabled = true)
    public void restartExecution() {
        DataFeed df = datafeedService.findDataFeedByName(MultiTenantContext.getTenant().getId(), dataFeedName);
        DataFeedExecution exec0 = df.getActiveExecution();
        assertEquals(exec0.getStatus(), DataFeedExecution.Status.Completed);
        int size = exec0.getImports().size();
        assertEquals(size, 1);

        exec0.setWorkflowId(2L);
        exec0.setStatus(DataFeedExecution.Status.Started);
        datafeedExecutionEntityMgr.update(exec0);
        df.setStatus(Status.ProcessAnalyzing);
        datafeedEntityMgr.update(df);

        Long workflowId = datafeedService.restartExecution(MultiTenantContext.getTenant().getId(), dataFeedName,
                DataFeedExecutionJobType.PA);
        assertEquals(exec0.getWorkflowId(), workflowId);

        exec0 = datafeedExecutionEntityMgr.findByPid(exec0.getPid());
        assertEquals(exec0.getStatus(), DataFeedExecution.Status.Failed);

        DataFeedExecution exec1 = datafeedService.startExecution(MultiTenantContext.getTenant().getId(), dataFeedName,
                DataFeedExecutionJobType.PA, workflowId + 1L);
        df = datafeedService.findDataFeedByName(MultiTenantContext.getTenant().getId(), dataFeedName);
        assertEquals(df.getStatus(), Status.ProcessAnalyzing);

        assertEquals(exec1.getPid(), df.getActiveExecution().getPid());
        assertEquals(exec1.getPid(), new Long(exec0.getPid() + 1L));
        assertEquals(exec1.getStatus(), DataFeedExecution.Status.Started);
    }

    @Test(groups = "functional", enabled = true)
    public void testUnblock() {
        DataCollection dataCollection = new DataCollection();
        dataCollection.setName(NamingUtils.uuid("DATA_COLLECTION_NAME"));
        dataCollection.setTenant(MultiTenantContext.getTenant());
        dataCollection.setVersion(Version.Blue);
        dataCollectionEntityMgr.create(dataCollection);

        DataFeed feed = new DataFeed();
        feed.setName(NamingUtils.uuid("test_datafeed"));
        feed.setTenant(mainTestTenant);
        feed.setDataCollection(dataCollection);
        feed.setSchedulingGroup(SchedulingGroup.QATesting);
        datafeedEntityMgr.create(feed);

        Long workflowId = 999L;
        DataFeedExecution execution = new DataFeedExecution();
        execution.setDataFeed(feed);
        execution.setStatus(DataFeedExecution.Status.Started);
        execution.setWorkflowId(workflowId);
        execution.setDataFeedExecutionJobType(DataFeedExecutionJobType.PA);
        datafeedExecutionEntityMgr.create(execution);

        assertEquals(feed.getStatus(), Status.Initing);
        assertEquals(execution.getStatus(), DataFeedExecution.Status.Started);
        assertFalse(datafeedService.unblockPA(mainCustomerSpace, workflowId));
        feed.setStatus(Status.ProcessAnalyzing);
        feed = datafeedEntityMgr.updateStatus(feed);
        assertEquals(feed.getStatus(), Status.ProcessAnalyzing);
        assertTrue(datafeedService.unblockPA(mainCustomerSpace, workflowId));
        execution = datafeedExecutionEntityMgr.findByPid(execution.getPid());
        feed = datafeedEntityMgr.findByPid(feed.getPid());
        assertEquals(feed.getStatus(), Status.Active);
        assertEquals(feed.getSchedulingGroup(), SchedulingGroup.QATesting);
        assertEquals(execution.getStatus(), DataFeedExecution.Status.Failed);

        dataCollectionEntityMgr.delete(dataCollection);
        datafeedExecutionEntityMgr.delete(execution);
        datafeedEntityMgr.delete(feed);
    }
}
