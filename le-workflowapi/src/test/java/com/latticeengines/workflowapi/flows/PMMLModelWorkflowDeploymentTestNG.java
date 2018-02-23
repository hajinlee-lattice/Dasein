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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
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
import com.latticeengines.pls.entitymanager.ModelSummaryDownloadFlagEntityMgr;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.scoringapi.score.impl.TestPMMLScoring;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiDeploymentTestNGBase;

public class PMMLModelWorkflowDeploymentTestNG extends WorkflowApiDeploymentTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PMMLModelWorkflowDeploymentTestNG.class);

    @Autowired
    private ModelSummaryDownloadFlagEntityMgr modelSummaryDownloadFlagEntityMgr;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private TestPMMLScoring testPMMLScoring;

    private String pmmlHdfsPath = null;
    private String pivotValuesHdfsPath = null;
    protected String modelName;
    private String modelDisplayName;
    private int modelCount = 1;

    @Test(groups = "workflow", dataProvider = "pmmlFileNameProvider", enabled = true)
    public void testWorkflow(String pmmlFileName, String pivotValueFileName) throws Exception {
        MultiTenantContext.setTenant(mainTestTenant);
        setupFiles(mainTestCustomerSpace, pmmlFileName, pivotValueFileName);
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

        workflowService.registerJob(workflowConfig.getName(), applicationContext);
        WorkflowExecutionId workflowId = workflowService.start(workflowConfig);

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
                .workflow("pmmlModelWorkflow") //
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
