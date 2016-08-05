package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.io.IOException;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.leadprioritization.workflow.PMMLModelWorkflow;
import com.latticeengines.leadprioritization.workflow.PMMLModelWorkflowConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.CreatePMMLModelConfiguration;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;

public class PMMLModelWorkflowDeploymentTestNG extends PMMLModelWorkflowTestNGBase {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(PMMLModelWorkflowDeploymentTestNG.class);

    @Autowired
    private PMMLModelWorkflow pmmlModelWorkflow;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private TestPMMLScoring testPMMLScoring;

    protected String modelName;

    private String modelDisplayName;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupForPMMLModel();
    }

    @Test(groups = "deployment", enabled = false)
    public void testWorkflow() throws Exception {

        PMMLModelWorkflowConfiguration workflowConfig = generatePMMLModelWorkflowConfiguration();
        for (String key : workflowConfig.getConfigRegistry().keySet()) {
            if (key.equals(CreatePMMLModelConfiguration.class.getCanonicalName())) {
                ObjectMapper om = new ObjectMapper();
                CreatePMMLModelConfiguration modelConfig = om.readValue(workflowConfig.getConfigRegistry().get(key),
                        CreatePMMLModelConfiguration.class);
                modelName = modelConfig.getModelName();
                System.out.println(workflowConfig.getConfigRegistry().get(key));
                System.out.println("Model name = " + modelName);
            }
        }

        WorkflowExecutionId workflowId = workflowService.start(pmmlModelWorkflow.name(), workflowConfig);

        // Line below is example of how to restart a workflow from the last
        // failed step; also need to disable the setup
        // WorkflowExecutionId workflowId = workflowService.restart(new
        // WorkflowExecutionId(18L));
        waitForCompletion(workflowId);

        List<ModelSummary> summaries = modelSummaryEntityMgr.findAllValid();
        assertEquals(summaries.size(), 1);
        for (ModelSummary summary : summaries) {
            if (summary.getName().startsWith(modelName)) {
                assertEquals(summary.getStatus(), ModelSummaryStatus.INACTIVE);
                assertTrue(summary.getDisplayName().startsWith("PMML MODEL - "));
                modelDisplayName = summary.getDisplayName();
            }
        }

    }

    @Test(groups = "deployment", enabled = false, dependsOnMethods = { "testWorkflow" })
    public void scoreRecords() throws IOException, InterruptedException {

        Model model = testPMMLScoring.getModel(modelDisplayName, PMML_CUSTOMERSPACE, pmmlTenant);
        System.out.println(modelDisplayName + ", " + model.getModelId());
        Assert.assertNotNull(model.getModelId());
        testPMMLScoring.scoreRecords(model.getModelId(), PMML_CUSTOMERSPACE, pmmlTenant);
    }

}
