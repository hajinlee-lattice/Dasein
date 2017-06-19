package com.latticeengines.workflowapi.flows;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.FitModelWorkflowConfiguration;

public class FitModelWorkflowInContainerDeploymentTestNG extends FitModelWorkflowTestNGBase {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(FitModelWorkflowInContainerDeploymentTestNG.class);

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
