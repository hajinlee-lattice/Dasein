package com.latticeengines.workflowapi.flows;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.leadprioritization.workflow.PMMLModelWorkflow;
import com.latticeengines.leadprioritization.workflow.PMMLModelWorkflowConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.CreatePMMLModelConfiguration;

public class PMMLModelWorkflowDeploymentTestNG extends PMMLModelWorkflowTestNGBase {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(PMMLModelWorkflowDeploymentTestNG.class);

    @Autowired
    private PMMLModelWorkflow pmmlModelWorkflow;

    @Autowired
    private TestPMMLScoring testPMMLScoring;

    protected String modelName;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupForPMMLModel();
    }

    @Test(groups = "deployment", enabled = true)
    public void testWorkflow() throws Exception {

        PMMLModelWorkflowConfiguration workflowConfig = generatePMMLModelWorkflowConfiguration();
        for (String key : workflowConfig.getConfigRegistry().keySet()) {
            if (key.equals(CreatePMMLModelConfiguration.class.getCanonicalName())) {
                ObjectMapper om = new ObjectMapper();
                CreatePMMLModelConfiguration modelConfig = om.readValue(
                        workflowConfig.getConfigRegistry().get(key),
                        CreatePMMLModelConfiguration.class);
                modelName = modelConfig.getModelName();
                System.out.println(workflowConfig.getConfigRegistry().get(key));
                System.out.println("Model name = " + modelName);
            }
        }

        WorkflowExecutionId workflowId = workflowService.start(pmmlModelWorkflow.name(),
                workflowConfig);

        // Line below is example of how to restart a workflow from the last
        // failed step; also need to disable the setup
        // WorkflowExecutionId workflowId = workflowService.restart(new
        // WorkflowExecutionId(18L));
        System.out.println("Workflow id = " + workflowId.getId());
        BatchStatus status = workflowService
                .waitForCompletion(workflowId, WORKFLOW_WAIT_TIME_IN_MILLIS).getStatus();
        Assert.assertEquals(status, BatchStatus.COMPLETED);

    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testWorkflow" })
    public void scoreRecords() throws IOException, InterruptedException {
        // wait for 15 seconds to make sure that model entry is created in model
        // summary
        Thread.sleep(15 * 1000);

        Model model = testPMMLScoring.getModel(modelName, PMML_CUSTOMERSPACE, pmmlTenant);
        System.out.println(modelName + ", " + model.getModelId());
        Assert.assertNotNull(model.getModelId());
        testPMMLScoring.scoreRecords(model.getModelId(), PMML_CUSTOMERSPACE, pmmlTenant);
    }

}
