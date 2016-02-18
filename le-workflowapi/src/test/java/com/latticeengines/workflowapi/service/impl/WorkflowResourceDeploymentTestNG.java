package com.latticeengines.workflowapi.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.workflowapi.flows.ModelWorkflow;
import com.latticeengines.workflowapi.flows.ModelWorkflowConfiguration;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;
import com.latticeengines.workflowapi.steps.dlorchestration.ModelLoadDataConfiguration;

public class WorkflowResourceDeploymentTestNG extends WorkflowApiFunctionalTestNGBase {

    @Autowired
    private ModelWorkflow modelWorkflow;

    @Test(groups = "deployment", enabled = true)
    public void submitWorkflow() throws Exception {
        ModelLoadDataConfiguration loadDataConfig = new ModelLoadDataConfiguration();
        loadDataConfig.setI(77);

        ModelWorkflowConfiguration workflowConfig = new ModelWorkflowConfiguration.Builder()
                .setModelLoadDataConfiguration(loadDataConfig).build();
        workflowConfig.setContainerConfiguration(modelWorkflow.name(), WFAPITEST_CUSTOMERSPACE,
                "WorkflowResourceDeploymentTest_submitWorkflow");

        submitWorkflowAndAssertSuccessfulCompletion(workflowConfig);
    }

}
