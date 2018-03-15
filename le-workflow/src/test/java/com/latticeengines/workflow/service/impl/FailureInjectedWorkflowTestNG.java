package com.latticeengines.workflow.service.impl;

import static org.testng.Assert.assertEquals;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.FailingStep;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflow.functionalframework.DynamicWorkflowChoreographer;
import com.latticeengines.workflow.functionalframework.FailureInjectedWorkflow;
import com.latticeengines.workflow.functionalframework.FailureInjectedWorkflowConfiguration;
import com.latticeengines.workflow.functionalframework.InjectedFailureListener;
import com.latticeengines.workflow.functionalframework.NamedStep;
import com.latticeengines.workflow.functionalframework.WorkflowTestNGBase;

public class FailureInjectedWorkflowTestNG extends WorkflowTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DynamicWorkflowTestNG.class);

    @Inject
    private WorkflowService workflowService;

    @Inject
    private TenantService tenantService;

    @Inject
    private FailureInjectedWorkflow failureInjectedWorkflow;

    @Inject
    private DynamicWorkflowChoreographer choreographer;

    @Resource(name = "stepA")
    private NamedStep stepA;

    @Resource(name = "stepB")
    private NamedStep stepB;

    @Resource(name = "stepC")
    private NamedStep stepC;

    @Resource(name = "stepD")
    private NamedStep stepD;

    @Inject
    private InjectedFailureListener failureListener;

    private WorkflowConfiguration workflowConfig;

    private String customerSpace;

    private WorkflowExecutionId workflowId;

    @BeforeClass(groups = "functional")
    public void setup() {
        customerSpace = bootstrapWorkFlowTenant().toString();
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        Tenant tenant = tenantService.findByTenantId(customerSpace);
        if (tenant != null) {
            tenantService.discardTenant(tenant);
        }
        super.cleanup(workflowId.getId());
    }

    // use this test to get complete step list from log
    @Test(groups = "functional", enabled = false)
    public void testNoFailure() throws Exception {
        workflowConfig = new FailureInjectedWorkflowConfiguration.Builder().build();
        workflowConfig.setCustomerSpace(CustomerSpace.parse(customerSpace));
        workflowService.registerJob(workflowConfig, applicationContext);
        try {
            workflowId = workflowService.start(workflowConfig);
            BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT, 1000).getStatus();
            assertEquals(status, BatchStatus.COMPLETED);
        } finally {
            workflowService.unRegisterJob(workflowConfig.getWorkflowName());
        }
    }

    @Test(groups = "functional", dataProvider = "injectedFailuresProvider")
    public void testInjectFailures(FailingStep failingStep, int expectedFailedSeq) throws Exception {
        workflowConfig = new FailureInjectedWorkflowConfiguration.Builder().build();
        workflowConfig.setCustomerSpace(CustomerSpace.parse(customerSpace));
        workflowConfig.setFailingStep(failingStep);
        workflowService.registerJob(workflowConfig, applicationContext);
        try {
            workflowId = workflowService.start(workflowConfig);
            BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT, 1000).getStatus();
            assertEquals(status, BatchStatus.FAILED);
            String message = failureListener.exitDescription;
            Assert.assertNotNull(message);
            String token = String.format("[%d]", expectedFailedSeq);
            Assert.assertTrue(message.contains(token), "Error message should contains " + token);
        } finally {
            workflowService.unRegisterJob(workflowConfig.getWorkflowName());
        }
    }

    @DataProvider(name = "injectedFailuresProvider")
    private Object[][] provideInjectedFailures() {
        FailingStep failStep0 = new FailingStep();
        failStep0.setSeq(0);

        FailingStep failStep6 = new FailingStep();
        failStep6.setSeq(6);

        FailingStep failFirstStepB= new FailingStep();
        failFirstStepB.setName(stepB.name());

        FailingStep failSecondStepD= new FailingStep();
        failSecondStepD.setName(stepD.name());
        failSecondStepD.setOccurrence(2);

        return new Object[][]{ //
                {failStep0, 0}, //
                {failStep6, 6}, //
                {failFirstStepB, 1}, //
                {failSecondStepD, 8}, //
        };
    }


}
