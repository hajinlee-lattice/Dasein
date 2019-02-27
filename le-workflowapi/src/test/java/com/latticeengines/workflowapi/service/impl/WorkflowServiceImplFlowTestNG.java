package com.latticeengines.workflowapi.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowJobUpdate;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobUpdateEntityMgr;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflow.functionalframework.AnotherSuccessfulStep;
import com.latticeengines.workflow.functionalframework.FailableStep;
import com.latticeengines.workflow.functionalframework.FailableWorkflow;
import com.latticeengines.workflow.functionalframework.FailingListener;
import com.latticeengines.workflow.functionalframework.SleepableStep;
import com.latticeengines.workflow.functionalframework.SleepableWorkflow;
import com.latticeengines.workflow.functionalframework.SuccessfulListener;
import com.latticeengines.workflow.functionalframework.SuccessfulStep;
import com.latticeengines.workflow.functionalframework.WorkflowWithFailingListener;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;
import com.latticeengines.workflowapi.service.WorkflowJobService;

public class WorkflowServiceImplFlowTestNG extends WorkflowApiFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(WorkflowServiceImplFlowTestNG.class);
    @Inject
    private WorkflowService workflowService;

    @Inject
    private FailableStep failableStep;

    @Inject
    private FailableWorkflow failableWorkflow;

    @Inject
    private SleepableStep sleepableStep;

    @Inject
    private SleepableWorkflow sleepableWorkflow;

    @Inject
    private AnotherSuccessfulStep anotherSuccessfulStep;

    @Inject
    private TenantService tenantService;

    @Inject
    private SuccessfulStep successfulStep;

    @Inject
    private WorkflowWithFailingListener workflowWithFailingListener;

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Inject
    private WorkflowJobService workflowApiWorkflowJobService;

    @Inject
    private WorkflowJobUpdateEntityMgr workflowJobUpdateEntityMgr;

    @Resource(name = "jdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    private WorkflowConfiguration failableWorkflowConfig;

    private WorkflowConfiguration sleepableWorkflowConfig;

    private WorkflowConfiguration workflowWithFailingListenerConfig;

    private Tenant tenant1;

    private String customerSpace;

    private Long workflowId;

    private ScheduledExecutorService executorService;

    @BeforeClass(groups = "functional")
    public void setup() {
        customerSpace = bootstrapWorkFlowTenant().toString();
        executorService = Executors.newSingleThreadScheduledExecutor();
        failableWorkflowConfig = new WorkflowConfiguration();
        failableWorkflowConfig.setCustomerSpace(CustomerSpace.parse(customerSpace));
        failableWorkflowConfig.setWorkflowName(failableWorkflow.name());
        workflowService.registerJob(failableWorkflowConfig, applicationContext);

        sleepableWorkflowConfig = new WorkflowConfiguration();
        sleepableWorkflowConfig.setCustomerSpace(CustomerSpace.parse(customerSpace));
        sleepableWorkflowConfig.setWorkflowName(sleepableWorkflow.name());
        workflowService.registerJob(sleepableWorkflowConfig, applicationContext);

        workflowWithFailingListenerConfig = new WorkflowConfiguration();
        workflowWithFailingListenerConfig.setCustomerSpace(CustomerSpace.parse(customerSpace));
        workflowWithFailingListenerConfig.setWorkflowName(workflowWithFailingListener.name());
        workflowService.registerJob(workflowWithFailingListenerConfig, applicationContext);
    }

    @AfterClass(groups = "functional")
    public void teardown() {
        tenant1 = tenantService.findByTenantId(customerSpace);
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }

        try {
            executorService.shutdown();
            executorService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (Exception exc) {
            log.warn("Exception while trying to shutdown executorService. " + exc);
        }
    }

    @BeforeMethod(groups = "functional")
    public void setupMethod() {

    }

    @AfterMethod(groups = "functional")
    public void cleanp() {
        WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowId(workflowId);
        if (workflowJob != null) {
            WorkflowJobUpdate jobUpdate = workflowJobUpdateEntityMgr.findByWorkflowPid(workflowJob.getPid());
            workflowJobEntityMgr.delete(workflowJob);
            workflowJobUpdateEntityMgr.delete(jobUpdate);
        }
    }

    @Test(groups = "functional")
    public void testStart() throws Exception {
        failableStep.setFail(false);
        WorkflowExecutionId workflowId = workflowService.start(failableWorkflowConfig);
        this.workflowId = workflowId.getId();
        ScheduledFuture<?> future = checkWorkflowJobUpdate(workflowId);
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT, 3000).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
        future.cancel(true);
    }

    @Test(groups = "functional", enabled = true)
    public void testGetJobs() throws Exception {
        failableStep.setFail(false);
        WorkflowExecutionId workflowId = workflowService.start(failableWorkflowConfig);
        this.workflowId = workflowId.getId();
        ScheduledFuture<?> future = checkWorkflowJobUpdate(workflowId);
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT, 3000).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
        List<Job> jobs = workflowApiWorkflowJobService.getJobsByWorkflowIds(customerSpace,
                Collections.singletonList(workflowId.getId()), Collections.singletonList(failableWorkflow.name()),
                false, false, null);
        assertEquals(jobs.size(), 1);
        Job job = jobs.get(0);
        assertEquals(job.getId().longValue(), workflowId.getId());
        assertEquals(job.getJobType(), failableWorkflow.name());
        assertEquals(job.getJobStatus(), JobStatus.COMPLETED);
        future.cancel(true);
    }

    @Test(groups = "functional")
    public void testRestart() throws Exception {
        failableStep.setFail(true);
        WorkflowExecutionId workflowId = workflowService.start(failableWorkflowConfig);
        this.workflowId = workflowId.getId();
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT, 3000).getStatus();
        List<String> stepNames = workflowService.getStepNames(workflowId);
        assertTrue(stepNames.contains(successfulStep.name()));
        assertTrue(stepNames.contains(failableStep.name()));
        assertFalse(stepNames.contains(anotherSuccessfulStep.name()));
        assertEquals(status, BatchStatus.FAILED);

        failableStep.setFail(false);
        String appid = "appid_2";
        WorkflowJob workflowJob = new WorkflowJob();
        workflowJob.setTenant(MultiTenantContext.getTenant());
        workflowJob.setApplicationId(appid);
        workflowJobEntityMgr.create(workflowJob);
        WorkflowExecutionId restartedWorkflowId = workflowService.restart(workflowId, workflowJob);
        status = workflowService.waitForCompletion(restartedWorkflowId, MAX_MILLIS_TO_WAIT, 3000).getStatus();
        List<String> restartedStepNames = workflowService.getStepNames(restartedWorkflowId);
        assertFalse(restartedStepNames.contains(successfulStep.name()));
        assertTrue(restartedStepNames.contains(failableStep.name()));
        assertTrue(restartedStepNames.contains(anotherSuccessfulStep.name()));
        assertEquals(status, BatchStatus.COMPLETED);
        assertEquals(workflowJobEntityMgr.findByApplicationId(appid).getWorkflowId().longValue(),
                restartedWorkflowId.getId());

        super.cleanup(workflowJob.getWorkflowId());
    }

    @Test(groups = "functional")
    public void testFailureReporting() throws Exception {
        failableStep.setFail(true);
        WorkflowExecutionId workflowId = workflowService.start(failableWorkflowConfig);
        this.workflowId = workflowId.getId();
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT, 3000).getStatus();
        assertEquals(status, BatchStatus.FAILED);
        Job job = workflowApiWorkflowJobService.getJobByWorkflowId(customerSpace, workflowId.getId(), false);
        assertEquals(job.getErrorCode(), LedpCode.LEDP_28001);
        assertNotNull(job.getErrorMsg());
    }

    @Test(groups = "functional")
    public void testFailureReportingWithGenericError() throws Exception {
        failableStep.setFail(true);
        failableStep.setUseRuntimeException(true);

        WorkflowExecutionId workflowId = workflowService.start(failableWorkflowConfig);
        this.workflowId = workflowId.getId();
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT, 3000).getStatus();
        assertEquals(status, BatchStatus.FAILED);
        Job job = workflowApiWorkflowJobService.getJobByWorkflowId(customerSpace, workflowId.getId(), false);
        assertEquals(job.getErrorCode(), LedpCode.LEDP_00002);
        assertNotNull(job.getErrorMsg());
    }

    @Test(groups = "functional")
    public void testWorkflowWithFailingListener() throws Exception {
        int successfulListenerCalls = SuccessfulListener.calls;
        int failureListenerCalls = FailingListener.calls;

        WorkflowExecutionId workflowId = workflowService.start(workflowWithFailingListenerConfig);
        this.workflowId = workflowId.getId();
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT, 3000).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
        assertEquals(SuccessfulListener.calls, successfulListenerCalls + 1);
        assertEquals(FailingListener.calls, failureListenerCalls + 1);
    }

    @Test(groups = "functional")
    public void testSleepableWF() throws Exception {
        sleepableStep.setSleepTime(80000L);

        List<Map<String, Object>> props = jdbcTemplate.queryForList("show variables like '%wait_timeout%'");
        log.info("Wait Timeout Properties for DB: " + props);
        WorkflowExecutionId workflowId = workflowService.start(sleepableWorkflowConfig);
        this.workflowId = workflowId.getId();
        BatchStatus status = workflowService.getStatus(workflowId).getStatus();
        assertTrue(status.equals(BatchStatus.STARTING) || status.equals(BatchStatus.STARTED));

        status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT, 3000).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }

    @Test(groups = "functional")
    public void testStop() throws Exception {
        sleepableStep.setSleepTime(10000L);

        WorkflowExecutionId workflowId = workflowService.start(sleepableWorkflowConfig);
        this.workflowId = workflowId.getId();
        BatchStatus status = workflowService.getStatus(workflowId).getStatus();
        assertTrue(status.equals(BatchStatus.STARTING) || status.equals(BatchStatus.STARTED));

        Thread.sleep(1500L);

        workflowService.stop(workflowId);
        List<String> stepNames = workflowService.getStepNames(workflowId);
        log.info("stepnames: " + stepNames);
        assertTrue(stepNames.contains(successfulStep.name()));
        assertTrue(stepNames.contains(sleepableStep.name()));
        assertFalse(stepNames.contains(anotherSuccessfulStep.name()));

        status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT, 3000).getStatus();
        assertEquals(status, BatchStatus.STOPPED);
    }

    private ScheduledFuture<?> checkWorkflowJobUpdate(WorkflowExecutionId workflowId) {
        Long workflowPid = workflowJobEntityMgr.findByWorkflowId(workflowId.getId()).getPid();
        WorkflowJobUpdate jobUpdate = workflowJobUpdateEntityMgr.findByWorkflowPid(workflowPid);
        assertNotNull(jobUpdate);
        Long diff = System.currentTimeMillis() - jobUpdate.getLastUpdateTime();
        assertTrue(diff < TimeUnit.SECONDS.toMillis(2));

        AtomicLong lastUpdateTime = new AtomicLong(jobUpdate.getLastUpdateTime());
        return executorService.scheduleAtFixedRate(() -> {
            Long updateTime = workflowJobUpdateEntityMgr.findByWorkflowPid(workflowPid).getLastUpdateTime();
            log.info(String.format("workflowPid = %s, updateTime = %s, lastUpdateTime = %s", workflowPid, updateTime,
                    lastUpdateTime));
            assertTrue(updateTime >= lastUpdateTime.getAndSet(updateTime));
        }, 2L, 2L, TimeUnit.SECONDS);
    }
}
