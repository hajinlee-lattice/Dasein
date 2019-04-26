package com.latticeengines.apps.cdl.testframework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.workflowapi.flows.testflows.framework.sampletests.SamplePostprocessingStepConfiguration;
import com.latticeengines.workflowapi.flows.testflows.framework.sampletests.SamplePreprocessingStepConfiguration;
import com.latticeengines.workflowapi.flows.testflows.framework.sampletests.SampleWorkflowToTestConfiguration;

public class CDLWorkflowFrameworkFunctionalTestNG extends CDLWorkflowFrameworkFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CDLWorkflowFrameworkFunctionalTestNG.class);

    @BeforeClass(groups = "functional" )
    public void setup() throws Exception {
        setupTestEnvironment();
        log.info("In CDLWorkflowFrameworkFunctionalTestNG.setup");
    }

    @Test(groups = "functional")
    public void testWorkflow() throws Exception {
        log.info("In CDLWorkflowFrameworkFunctionalTestNG.testWorkflow");

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
        log.info("In CDLWorkflowFrameworkFunctionalTestNG.verifyTest");
    }

    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        log.info("In CDLWorkflowFrameworkFunctionalTestNG.tearDown");
    }

}
