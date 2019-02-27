package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.serviceflows.modeling.PMMLModelWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.CreatePMMLModelConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.scoringapi.score.impl.TestPMMLScoring;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiDeploymentTestNGBase;

public class PMMLModelWorkflowDeploymentTestNG extends WorkflowApiDeploymentTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PMMLModelWorkflowDeploymentTestNG.class);

    @Autowired
    private ModelSummaryProxy modelSummaryProxy;

    @Autowired
    private TestPMMLScoring testPMMLScoring;

    private String pmmlHdfsPath = null;
    private String pivotValuesHdfsPath = null;
    protected String modelName;
    private String modelDisplayName;
    private int modelCount = 1;
    private PMMLModelWorkflowConfiguration workflowConfig;

    @Override
    @BeforeClass(groups = { "deployment", "workflow" })
    public void setup() throws Exception {
        setupTestEnvironment(LatticeProduct.LPA3);
    }

    @Test(groups = "workflow", dataProvider = "pmmlFileNameProvider", enabled = true)
    public void testWorkflow(String pmmlFileName, String pivotValueFileName) throws Exception {
        MultiTenantContext.setTenant(mainTestTenant);
        setupFiles(mainTestCustomerSpace, pmmlFileName, pivotValueFileName);

        workflowConfig = generatePMMLModelWorkflowConfiguration();
        workflowService.unRegisterJob(workflowConfig.getWorkflowName());
        workflowService.registerJob(workflowConfig, applicationContext);

        String key = CreatePMMLModelConfiguration.class.getSimpleName();
        CreatePMMLModelConfiguration modelConfig = JsonUtils
                .deserialize(workflowConfig.getStepConfigRegistry().get(key), CreatePMMLModelConfiguration.class);
        modelName = modelConfig.getModelName();
        System.out.println(workflowConfig.getStepConfigRegistry());
        System.out.println("Model name = " + modelName);

        modelSummaryProxy.setDownloadFlag(MultiTenantContext.getTenant().getId());

        WorkflowExecutionId workflowId = workflowService.start(workflowConfig);
        waitForCompletion(workflowId);

        List<ModelSummary> summaries = modelSummaryProxy.findAllValid(mainTestTenant.getId());
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

    private void scoreRecords() throws IOException, InterruptedException {
        Model model = testPMMLScoring.getModel(modelDisplayName, mainTestCustomerSpace, mainTestTenant);
        System.out.println(modelDisplayName + ", " + model.getModelId());
        Assert.assertNotNull(model.getModelId());
        testPMMLScoring.scoreRecords(model.getModelId(), mainTestCustomerSpace, mainTestTenant);
    }

    @DataProvider(name = "pmmlFileNameProvider")
    public Object[][] getDataProvider() {
        return new Object[][] { //
                { "rfpmml.xml", "pivotvalues.txt" }, //
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

    private void setupFiles(CustomerSpace customerSpace, String pmmlFileName, String pivotFileName) throws Exception {
        URL pmmlFile = ClassLoader
                .getSystemResource("com/latticeengines/workflowapi/flows/leadprioritization/pmmlfiles/" + pmmlFileName);
        Path pmmlFolderHdfsPath = PathBuilder.buildMetadataPathForArtifactType(CamilleEnvironment.getPodId(), //
                customerSpace, "module1", ArtifactType.PMML);
        pmmlHdfsPath = null;
        pmmlHdfsPath = pmmlFolderHdfsPath.toString() + "/" + new File(pmmlFile.getFile()).getName();
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, pmmlFile.getPath(), pmmlHdfsPath);
        pivotValuesHdfsPath = null;
        if (StringUtils.isNotEmpty(pivotFileName)) {
            URL pivotFile = ClassLoader.getSystemResource(
                    "com/latticeengines/workflowapi/flows/leadprioritization/pivotfiles/" + pivotFileName);

            Path pivotValuesFolderHdfsPath = PathBuilder.buildMetadataPathForArtifactType(CamilleEnvironment.getPodId(), //
                    customerSpace, "module1", ArtifactType.PivotMapping);

            pivotValuesHdfsPath = pivotValuesFolderHdfsPath.toString() + "/" + new File(pivotFile.getFile()).getName();
            HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, pivotFile.getPath(), pivotValuesHdfsPath);
        }
    }

    private PMMLModelWorkflowConfiguration generatePMMLModelWorkflowConfiguration() {
        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "pmmlModelWorkflow");

        PMMLModelWorkflowConfiguration workflowConfig = new PMMLModelWorkflowConfiguration.Builder() //
                .podId(CamilleEnvironment.getPodId()) //
                .microServiceHostPort(microServiceHostPort) //
                .customer(mainTestCustomerSpace) //
                .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .modelName("PMMLModel-" + System.currentTimeMillis()) //
                .pmmlArtifactPath(pmmlHdfsPath) //
                .pivotArtifactPath(pivotValuesHdfsPath) //
                .inputProperties(inputProperties) //
                .internalResourceHostPort(internalResourceHostPort) //
                .sourceSchemaInterpretation(SchemaInterpretation.SalesforceLead.name()) //
                .displayName("PMML MODEL - " + new Path(pmmlHdfsPath).getSuffix()) //
                .build();

        return workflowConfig;
    }
}
