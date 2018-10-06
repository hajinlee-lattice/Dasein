package com.latticeengines.apps.cdl.testframework;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.yarn.client.YarnClient;

import com.latticeengines.apps.cdl.service.impl.CheckpointService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflowapi.flows.testflows.framework.TestBasePostprocessingStepConfiguration;
import com.latticeengines.workflowapi.flows.testflows.framework.TestBasePreprocessingStepConfiguration;
import com.latticeengines.workflowapi.flows.testflows.framework.TestFrameworkWrapperWorkflowConfiguration;
import com.latticeengines.yarn.functionalframework.YarnFunctionalTestNGBase;

import static org.testng.Assert.assertEquals;

@ContextConfiguration(locations = { "classpath:test-serviceapps-cdl-workflow-context.xml" })
public abstract class CDLWorkflowFrameworkTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(CDLWorkflowFrameworkTestNGBase.class);

    protected static final long WORKFLOW_WAIT_TIME_IN_MILLIS = 1000L * 60 * 90;

    protected Tenant mainTestTenant;
    protected CustomerSpace mainTestCustomerSpace;

    @Inject
    protected CheckpointService checkpointService;

    protected YarnFunctionalTestNGBase platformTestBase;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected YarnClient defaultYarnClient;

    @Autowired
    protected WorkflowService workflowService;

    abstract public void setup() throws Exception;

    abstract public void testWorkflow() throws Exception;

    abstract protected void verifyTest();

    abstract public void tearDown() throws Exception;

    // This version of generateConfiguration is used for setting up a test for a single Workflow Step rather than a
    // full Workflow.
    protected TestFrameworkWrapperWorkflowConfiguration generateStepTestConfiguration(
            TestBasePreprocessingStepConfiguration preprocessStepConfiguration,
            String stepUnderTestBeanName,
            BaseStepConfiguration testStepConfiguration,
            TestBasePostprocessingStepConfiguration postprocessStepConfiguration) {
        TestFrameworkWrapperWorkflowConfiguration.Builder frameworkWorkflowBuilder =
                new TestFrameworkWrapperWorkflowConfiguration.Builder(true);
        return frameworkWorkflowBuilder //
                .setTestPreprocessStepConfiguration(preprocessStepConfiguration) //
                .setTestStepBeanName(stepUnderTestBeanName) //
                .setTestStepConfiguration(testStepConfiguration) //
                .setTestPostprocessStepConfiguration(postprocessStepConfiguration) //
                .customer(mainTestCustomerSpace) //
                .build();
    }

    // This version of generateConfiguration is used for setting up a test for a full Workflow rather than a single
    // Workflow Step.
    protected TestFrameworkWrapperWorkflowConfiguration generateWorkflowTestConfiguration(
            TestBasePreprocessingStepConfiguration preprocessStepConfiguration,
            String workflowUnderTestName,
            WorkflowConfiguration testWorkflowConfiguration,
            TestBasePostprocessingStepConfiguration postprocessStepConfiguration) {
        TestFrameworkWrapperWorkflowConfiguration.Builder frameworkWorkflowBuilder =
                new TestFrameworkWrapperWorkflowConfiguration.Builder(false);
        return frameworkWorkflowBuilder //
                .setTestPreprocessStepConfiguration(preprocessStepConfiguration) //
                .setTestWorkflowName(workflowUnderTestName) //
                .setTestWorkflowConfiguration(testWorkflowConfiguration) //
                .setTestPostprocessStepConfiguration(postprocessStepConfiguration) //
                .customer(mainTestCustomerSpace) //
                .build();
    }

    protected void runWorkflow(WorkflowConfiguration workflowConfig) throws Exception {
        workflowService.registerJob(workflowConfig, applicationContext);
        WorkflowExecutionId workflowId = workflowService.start(workflowConfig);
        BatchStatus status = workflowService.waitForCompletion(workflowId, WORKFLOW_WAIT_TIME_IN_MILLIS,
                1000).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }
}
