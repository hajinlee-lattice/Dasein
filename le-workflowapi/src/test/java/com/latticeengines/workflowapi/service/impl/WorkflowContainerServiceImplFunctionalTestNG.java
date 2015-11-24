package com.latticeengines.workflowapi.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.workflowapi.flows.ModelWorkflow;
import com.latticeengines.workflowapi.flows.ModelWorkflowConfiguration;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;
import com.latticeengines.workflowapi.service.WorkflowContainerService;
import com.latticeengines.workflowapi.steps.dlorchestration.ModelLoadDataConfiguration;

public class WorkflowContainerServiceImplFunctionalTestNG extends WorkflowApiFunctionalTestNGBase {

    @Autowired
    private WorkflowContainerService workflowContainerService;

    @Autowired
    private ModelWorkflow modelWorkflow;

    @Test(groups = "functional")
    public void submitWorkflow() throws Exception {
        ModelLoadDataConfiguration loadDataConfig = new ModelLoadDataConfiguration();
        loadDataConfig.setI(77);

        ModelWorkflowConfiguration workflowConfig = new ModelWorkflowConfiguration.Builder()
                .setModelLoadDataConfiguration(loadDataConfig).build();
        workflowConfig.setContainerConfiguration(modelWorkflow.name(), WFAPITEST_CUSTOMERSPACE,
                "WorkflowContainerServiceImplTest_submitWorkflow");

        ApplicationId appId = workflowContainerService.submitWorkFlow(workflowConfig);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

}
