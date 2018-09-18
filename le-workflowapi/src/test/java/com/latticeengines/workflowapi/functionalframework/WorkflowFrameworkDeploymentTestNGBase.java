package com.latticeengines.workflowapi.functionalframework;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflowapi.flows.testflows.framework.TestBasePostprocessingStepConfiguration;
import com.latticeengines.workflowapi.flows.testflows.framework.TestBasePreprocessingStepConfiguration;
import com.latticeengines.workflowapi.flows.testflows.framework.TestFrameworkWrapperWorkflowConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.admin.LatticeProduct;

// Abstract base class providing common functionality for single step and single workflow deployment tests.
public abstract class WorkflowFrameworkDeploymentTestNGBase extends WorkflowApiDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(WorkflowFrameworkDeploymentTestNGBase.class);

    @Override
    public void setup() throws Exception {
        setupTestEnvironment(LatticeProduct.LPA3);

        // TODO: Do we need this?
        //tenantName = mainTestCustomerSpace.getTenantId();
    }

    public void testWorkflow() throws Exception {

    }

    protected void verifyTest() {
        log.error("In WorkflowFrameworkDeploymentTestNGBase.verifyTest");
    }

    @Override
    public void tearDown() throws Exception {

    }

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
                .microServiceHostPort(microServiceHostPort) //
                .internalResourceHostPort(internalResourceHostPort) //
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
                .microServiceHostPort(microServiceHostPort) //
                .internalResourceHostPort(internalResourceHostPort) //
                .build();
    }


}
