package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertEquals;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.FitModelWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;

public class FitModelWorkflowDeploymentTestNG extends FitModelWorkflowTestNGBase {

    @Autowired
    private SoftwareLibraryService softwareLibraryService;

    @Autowired
    private VersionManager versionManager;

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(FitModelWorkflowDeploymentTestNG.class);

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupForFitModel();
    }

    @Test(groups = "deployment", enabled = true)
    public void testWorkflow() throws Exception {
        FitModelWorkflowConfiguration workflowConfig = generateFitModelWorkflowConfiguration();

        applicationContext = softwareLibraryService.loadSoftwarePackages("workflowapi", applicationContext,
                versionManager);

        workflowService.registerJob(workflowConfig.getWorkflowName(), applicationContext);
        WorkflowExecutionId workflowId = workflowService.start(workflowConfig.getWorkflowName(), workflowConfig);

        // Line below is example of how to restart a workflow from the last
        // failed step; also need to disable the setup
        // WorkflowExecutionId workflowId = workflowService.restart(new
        // WorkflowExecutionId(18L));
        System.out.println("Workflow id = " + workflowId.getId());
        BatchStatus status = workflowService.waitForCompletion(workflowId, WORKFLOW_WAIT_TIME_IN_MILLIS).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }

}
