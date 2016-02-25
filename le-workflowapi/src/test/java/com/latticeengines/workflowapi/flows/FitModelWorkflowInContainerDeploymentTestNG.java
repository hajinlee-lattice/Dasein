package com.latticeengines.workflowapi.flows;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.prospectdiscovery.workflow.FitModelWorkflow;
import com.latticeengines.prospectdiscovery.workflow.FitModelWorkflowConfiguration;

public class FitModelWorkflowInContainerDeploymentTestNG extends FitModelWorkflowTestNGBase {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(FitModelWorkflowInContainerDeploymentTestNG.class);

    @Autowired
    private FitModelWorkflow fitModelWorkflow;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupForFitModel();
    }

    @Test(groups = "deployment", enabled = false)
    public void testWorkflowInContainer() throws Exception {
        FitModelWorkflowConfiguration workflowConfig = generateFitModelWorkflowConfiguration();

        workflowConfig.setContainerConfiguration(fitModelWorkflow.name(), DEMO_CUSTOMERSPACE,
                "FitModelWorkflowTest_submitWorkflow");

        submitWorkflowAndAssertSuccessfulCompletion(workflowConfig);
    }
}
