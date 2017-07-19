package com.latticeengines.workflowapi.flows;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.FitModelWorkflowConfiguration;

@Deprecated
public class FitModelWorkflowInContainerDeploymentTestNG extends FitModelWorkflowTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(FitModelWorkflowInContainerDeploymentTestNG.class);

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupForFitModel();
    }

    @Test(groups = "deployment", enabled = false)
    public void testWorkflowInContainer() throws Exception {
        FitModelWorkflowConfiguration workflowConfig = generateFitModelWorkflowConfiguration();

        workflowConfig.setContainerConfiguration(workflowConfig.getWorkflowName(), DEMO_CUSTOMERSPACE,
                "FitModelWorkflowTest_submitWorkflow");

        submitWorkflowAndAssertSuccessfulCompletion(workflowConfig);
    }
}
