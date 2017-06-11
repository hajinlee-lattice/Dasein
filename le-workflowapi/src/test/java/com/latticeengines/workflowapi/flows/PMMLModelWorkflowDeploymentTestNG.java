package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.leadprioritization.workflow.PMMLModelWorkflow;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.PMMLModelWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.CreatePMMLModelConfiguration;
import com.latticeengines.pls.entitymanager.ModelSummaryDownloadFlagEntityMgr;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.scoringapi.score.impl.TestPMMLScoring;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class PMMLModelWorkflowDeploymentTestNG extends PMMLModelWorkflowTestNGBase {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(PMMLModelWorkflowDeploymentTestNG.class);

    @Autowired
    private PMMLModelWorkflow pmmlModelWorkflow;

    @Autowired
    private ModelSummaryDownloadFlagEntityMgr modelSummaryDownloadFlagEntityMgr;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private TestPMMLScoring testPMMLScoring;

    protected String modelName;

    private String modelDisplayName;

    private int modelCount = 1;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupForPMMLModel();
    }

    @AfterClass(groups = "deployment")
    public void cleanup() throws Exception {
        cleanUpAfterPMMLModel();
    }

    @Test(groups = "deployment", dataProvider = "pmmlFileNameProvider", enabled = true)
    public void testWorkflow(String pmmlFileName, String pivotValueFileName) throws Exception {
        setupFiles(PMML_CUSTOMERSPACE, pmmlFileName, pivotValueFileName);
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
        modelSummaryDownloadFlagEntityMgr.addDownloadFlag(MultiTenantContext.getTenant().getId());
        WorkflowExecutionId workflowId = workflowService.start(pmmlModelWorkflow.name(), workflowConfig);

        waitForCompletion(workflowId);

        List<ModelSummary> summaries = modelSummaryEntityMgr.findAllValid();
        assertEquals(summaries.size(), modelCount++);
        for (ModelSummary summary : summaries) {
            if (summary.getName().startsWith(modelName)) {
                assertEquals(summary.getStatus(), ModelSummaryStatus.INACTIVE);
                assertTrue(summary.getDisplayName().startsWith("PMML MODEL - "));
                modelDisplayName = summary.getDisplayName();
            }
        }
        scoreRecords();
    }

    public void scoreRecords() throws IOException, InterruptedException {

        Model model = testPMMLScoring.getModel(modelDisplayName, PMML_CUSTOMERSPACE, pmmlTenant);
        System.out.println(modelDisplayName + ", " + model.getModelId());
        Assert.assertNotNull(model.getModelId());
        testPMMLScoring.scoreRecords(model.getModelId(), PMML_CUSTOMERSPACE, pmmlTenant);
    }

    @DataProvider(name = "pmmlFileNameProvider")
    public Object[][] getDataProvider() {
        return new Object[][] { { "rfpmml.xml", "pivotvalues.txt" }, //
                { "dectree.xml", "" }, //
                { "glm_lead_pmml.xml", "GLM_test_mapping_table.csv" }, //
                { "lr.xml", "" }, //
                { "svm.xml", "" }, //
                { "svm_iris.xml", "" }, //
                { "naivebayes.xml", "" }, //
                { "IRIS_MLP_Neural_Network.xml", "" }, //
                { "nn.xml", "" } //
        };
    }
}
