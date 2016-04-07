package com.latticeengines.workflow.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflow.functionalframework.AnotherSuccessfulStep;
import com.latticeengines.workflow.functionalframework.FailableStep;
import com.latticeengines.workflow.functionalframework.FailableWorkflow;
import com.latticeengines.workflow.functionalframework.RunCompletedStepAgainWorkflow;
import com.latticeengines.workflow.functionalframework.SleepableStep;
import com.latticeengines.workflow.functionalframework.SleepableWorkflow;
import com.latticeengines.workflow.functionalframework.SuccessfulStep;
import com.latticeengines.workflow.functionalframework.WorkflowFunctionalTestNGBase;

public class WorkflowServiceImplTestNG extends WorkflowFunctionalTestNGBase {

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
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private SuccessfulStep successfulStep;

    private WorkflowConfiguration workflowConfig;

    private Tenant tenant1;

    private String customerSpace;

    @BeforeClass(groups = "functional")
    public void setup() {
        String tenantName = "Workflow_Tenant";
        customerSpace = CustomerSpace.parse(tenantName).toString();
        tenant1 = tenantService.findByTenantId(customerSpace);
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }

        workflowConfig = new WorkflowConfiguration();
        tenant1 = new Tenant();
        tenant1.setId(customerSpace);
        tenant1.setName(customerSpace);
        tenantEntityMgr.create(tenant1);
        workflowConfig.setCustomerSpace(CustomerSpace.parse(customerSpace));
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        tenant1 = tenantService.findByTenantId(customerSpace);
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
    }

    @Test(groups = "functional", enabled = true)
    public void testStart() throws Exception {
        failableStep.setFail(false);
        WorkflowExecutionId workflowId = workflowService.start(failableWorkflow.name(), workflowConfig);
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }

    @Test(groups = "functional", enabled = true)
    public void testGetJobs() throws Exception {
        failableStep.setFail(false);
        WorkflowExecutionId workflowId = workflowService.start(failableWorkflow.name(), workflowConfig);
        workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT).getStatus();
        List<Job> jobs = workflowService.getJobs(Arrays.asList(workflowId), failableWorkflow.name());
        assertEquals(jobs.size(), 1);
        Job job = jobs.get(0);
        assertEquals(job.getId().longValue(), workflowId.getId());
        assertEquals(job.getJobType(), failableWorkflow.name());
    }

    @Test(groups = "functional", enabled = true)
    public void testRestart() throws Exception {
        failableStep.setFail(true);
        WorkflowExecutionId workflowId = workflowService.start(failableWorkflow.name(), workflowConfig);
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT).getStatus();
        List<String> stepNames = workflowService.getStepNames(workflowId);
        assertTrue(stepNames.contains(successfulStep.name()));
        assertTrue(stepNames.contains(failableStep.name()));
        assertFalse(stepNames.contains(anotherSuccessfulStep.name()));
        assertEquals(status, BatchStatus.FAILED);

        failableStep.setFail(false);
        WorkflowExecutionId restartedWorkflowId = workflowService.restart(workflowId);
        status = workflowService.waitForCompletion(restartedWorkflowId, MAX_MILLIS_TO_WAIT).getStatus();
        List<String> restartedStepNames = workflowService.getStepNames(restartedWorkflowId);
        assertFalse(restartedStepNames.contains(successfulStep.name()));
        assertTrue(restartedStepNames.contains(failableStep.name()));
        assertTrue(restartedStepNames.contains(anotherSuccessfulStep.name()));
        assertEquals(status, BatchStatus.COMPLETED);
    }

    @Test(groups = "functional", enabled = true)
    public void testGetNames() throws Exception {
        List<String> workflowNames = workflowService.getNames();
        assertTrue(workflowNames.contains(failableWorkflow.name()));
        assertTrue(workflowNames.contains(runCompletedStepAgainWorkflow.name()));
    }

    @Test(groups = "functional", enabled = true)
    public void testStop() throws Exception {
        sleepableStep.setSleepTime(500L);
        WorkflowExecutionId workflowId = workflowService.start(sleepableWorkflow.name(), workflowConfig);
        BatchStatus status = workflowService.getStatus(workflowId).getStatus();
        assertTrue(status.equals(BatchStatus.STARTING) || status.equals(BatchStatus.STARTED));

        Thread.sleep(400L);

        workflowService.stop(workflowId);
        List<String> stepNames = workflowService.getStepNames(workflowId);

        assertTrue(stepNames.contains(successfulStep.name()));
        assertTrue(stepNames.contains(sleepableStep.name()));
        assertFalse(stepNames.contains(anotherSuccessfulStep.name()));

        status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT).getStatus();
        assertEquals(status, BatchStatus.STOPPED);
    }

}
