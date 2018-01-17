package com.latticeengines.workflow.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.*;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflow.functionalframework.AnotherSuccessfulStep;
import com.latticeengines.workflow.functionalframework.FailableStep;
import com.latticeengines.workflow.functionalframework.FailableWorkflow;
import com.latticeengines.workflow.functionalframework.FailingListener;
import com.latticeengines.workflow.functionalframework.RunCompletedStepAgainWorkflow;
import com.latticeengines.workflow.functionalframework.SleepableStep;
import com.latticeengines.workflow.functionalframework.SleepableWorkflow;
import com.latticeengines.workflow.functionalframework.SuccessfulListener;
import com.latticeengines.workflow.functionalframework.SuccessfulStep;
import com.latticeengines.workflow.functionalframework.WorkflowTestNGBase;
import com.latticeengines.workflow.functionalframework.WorkflowWithFailingListener;

public class WorkflowServiceImplTestNG extends WorkflowTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(WorkflowServiceImplTestNG.class);
    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private FailableStep failableStep;

    @Autowired
    private FailableWorkflow failableWorkflow;

    @Autowired
    private RunCompletedStepAgainWorkflow runCompletedStepAgainWorkflow;

    @Autowired
    private SleepableStep sleepableStep;

    @Autowired
    private SleepableWorkflow sleepableWorkflow;

    @Autowired
    private AnotherSuccessfulStep anotherSuccessfulStep;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private SuccessfulStep successfulStep;

    @Autowired
    private WorkflowWithFailingListener workflowWithFailingListener;

    private WorkflowConfiguration workflowConfig;

    private Tenant tenant1;

    private String customerSpace;

    @BeforeClass(groups = "functional")
    public void setup() {
        customerSpace = bootstrapWorkFlowTenant().toString();
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        tenant1 = tenantService.findByTenantId(customerSpace);
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
    }

    @BeforeMethod(groups = "functional")
    public void setupMethod() {
        workflowConfig = new WorkflowConfiguration();
        workflowConfig.setCustomerSpace(CustomerSpace.parse(customerSpace));
    }

    @Test(groups = "functional")
    public void testStart() throws Exception {
        failableStep.setFail(false);
        workflowConfig.setWorkflowName(failableWorkflow.name());
        WorkflowExecutionId workflowId = workflowService.start(workflowConfig);
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }

    @Test(groups = "functional")
    public void testGetJobs() throws Exception {
        failableStep.setFail(false);
        workflowConfig.setWorkflowName(failableWorkflow.name());
        WorkflowExecutionId workflowId = workflowService.start(workflowConfig);
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
        List<Job> jobs = workflowService.getJobs(Collections.singletonList(workflowId), failableWorkflow.name());
        assertEquals(jobs.size(), 1);
        Job job = jobs.get(0);
        assertEquals(job.getId().longValue(), workflowId.getId());
        assertEquals(job.getJobType(), failableWorkflow.name());
        assertEquals(job.getJobStatus(), JobStatus.COMPLETED);
    }

    @Test(groups = "functional")
    public void testRestart() throws Exception {
        failableStep.setFail(true);
        workflowConfig.setWorkflowName(failableWorkflow.name());
        WorkflowExecutionId workflowId = workflowService.start(workflowConfig);
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT).getStatus();
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
        status = workflowService.waitForCompletion(restartedWorkflowId, MAX_MILLIS_TO_WAIT).getStatus();
        List<String> restartedStepNames = workflowService.getStepNames(restartedWorkflowId);
        assertFalse(restartedStepNames.contains(successfulStep.name()));
        assertTrue(restartedStepNames.contains(failableStep.name()));
        assertTrue(restartedStepNames.contains(anotherSuccessfulStep.name()));
        assertEquals(status, BatchStatus.COMPLETED);
        assertEquals(workflowJobEntityMgr.findByApplicationId(appid).getWorkflowId().longValue(),
                restartedWorkflowId.getId());
    }

    @Test(groups = "functional")
    public void testFailureReporting() throws Exception {
        failableStep.setFail(true);
        workflowConfig.setWorkflowName(failableWorkflow.name());
        WorkflowExecutionId workflowId = workflowService.start(workflowConfig);
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT).getStatus();
        assertEquals(status, BatchStatus.FAILED);
        Job job = workflowService.getJob(workflowId);
        assertEquals(job.getErrorCode(), LedpCode.LEDP_28001);
        assertNotNull(job.getErrorMsg());
    }

    @Test(groups = "functional")
    public void testFailureReportingWithGenericError() throws Exception {
        failableStep.setFail(true);
        failableStep.setUseRuntimeException(true);
        workflowConfig.setWorkflowName(failableWorkflow.name());
        WorkflowExecutionId workflowId = workflowService.start(workflowConfig);
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT).getStatus();
        assertEquals(status, BatchStatus.FAILED);
        Job job = workflowService.getJob(workflowId);
        assertEquals(job.getErrorCode(), LedpCode.LEDP_00002);
        assertNotNull(job.getErrorMsg());
    }

    @Test(groups = "functional")
    public void testGetNames() throws Exception {
        List<String> workflowNames = workflowService.getNames();
        assertTrue(workflowNames.contains(failableWorkflow.name()));
        assertTrue(workflowNames.contains(runCompletedStepAgainWorkflow.name()));
    }

    @Test(groups = "functional")
    public void testWorkflowWithFailingListener() throws Exception {
        int successfulListenerCalls = SuccessfulListener.calls;
        int failureListenerCalls = FailingListener.calls;
        workflowConfig.setWorkflowName(workflowWithFailingListener.name());
        WorkflowExecutionId workflowId = workflowService.start(workflowConfig);
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
        assertEquals(SuccessfulListener.calls, successfulListenerCalls + 1);
        assertEquals(FailingListener.calls, failureListenerCalls + 1);
    }

    @Test(groups = "functional")
    public void testStop() throws Exception {
        sleepableStep.setSleepTime(10000L);
        workflowConfig.setWorkflowName(sleepableWorkflow.name());
        WorkflowExecutionId workflowId = workflowService.start(workflowConfig);
        BatchStatus status = workflowService.getStatus(workflowId).getStatus();
        assertTrue(status.equals(BatchStatus.STARTING) || status.equals(BatchStatus.STARTED));

        Thread.sleep(1500L);

        workflowService.stop(workflowId);
        List<String> stepNames = workflowService.getStepNames(workflowId);
        log.info("stepnames: " + stepNames);
        assertTrue(stepNames.contains(successfulStep.name()));
        assertTrue(stepNames.contains(sleepableStep.name()));
        assertFalse(stepNames.contains(anotherSuccessfulStep.name()));

        status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT).getStatus();
        assertEquals(status, BatchStatus.STOPPED);
    }
}
