package com.latticeengines.workflowapi.steps.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.workflowapi.flows.testflows.framework.sampletests.SamplePostprocessingStepConfiguration;
import com.latticeengines.workflowapi.flows.testflows.framework.sampletests.SamplePreprocessingStepConfiguration;
import com.latticeengines.workflowapi.flows.testflows.framework.sampletests.SampleWorkflowToTestConfiguration;
import com.latticeengines.workflowapi.functionalframework.WorkflowFrameworkDeploymentTestNGBase;

// Example deployment test class for creating a test for a single workflow.
public class SampleSingleWorkflowDeploymentTestNG extends WorkflowFrameworkDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SampleSingleWorkflowDeploymentTestNG.class);

    @Override
    @BeforeClass(groups = "deployment" )
    public void setup() throws Exception {
        super.setup();
    }

    @Test(groups = "deployment")
    public void testWorkflow() throws Exception {
        super.testWorkflow();

        SamplePreprocessingStepConfiguration preStepConfig = new SamplePreprocessingStepConfiguration(
                "samplePreprocessingStep");
        SamplePostprocessingStepConfiguration postStepConfig = new SamplePostprocessingStepConfiguration(
                "samplePostprocessingStep");

        // Test framework user must set up the configuration for the workflow under test because the framework cannot
        // assume how the configuration is set up.
        SampleWorkflowToTestConfiguration.Builder testWorkflowBuilder = new SampleWorkflowToTestConfiguration.Builder();
        SampleWorkflowToTestConfiguration testWorkflowConfig = testWorkflowBuilder
                .workflow("sampleWorkflowToTest")
                .customer(mainTestCustomerSpace)
                .build();

        runWorkflow(generateWorkflowTestConfiguration(preStepConfig, "sampleWorkflowToTest",
                testWorkflowConfig, postStepConfig));
        verifyTest();
    }

    @Override
    protected void verifyTest() {
        super.verifyTest();
    }

    @Override
    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        super.tearDown();
    }
}
