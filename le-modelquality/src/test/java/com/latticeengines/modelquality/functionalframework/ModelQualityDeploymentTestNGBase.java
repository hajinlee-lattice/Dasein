package com.latticeengines.modelquality.functionalframework;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.testng.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.modelquality.Algorithm;
import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.DataSetType;
import com.latticeengines.domain.exposed.modelquality.ModelConfig;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.ModelRunStatus;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PipelineStep;
import com.latticeengines.domain.exposed.modelquality.PipelineStepOrFile;
import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.domain.exposed.modelquality.Sampling;
import com.latticeengines.domain.exposed.modelquality.ScoringDataSet;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.modelquality.service.ModelRunService;
import com.latticeengines.proxy.exposed.modelquality.ModelQualityProxy;
import com.latticeengines.testframework.security.impl.GlobalAuthDeploymentTestBed;

import edu.emory.mathcs.backport.java.util.Arrays;

public class ModelQualityDeploymentTestNGBase extends ModelQualityTestNGBase {

    @Autowired
    protected ModelQualityProxy modelQualityProxy;

    @Value("${modelquality.test.pls.deployment.api}")
    protected String plsDeployedHostPort;

    @Value("${modelquality.test.admin.deployment.api}")
    protected String adminDeployedHostPort;

    @Autowired
    protected GlobalAuthDeploymentTestBed deploymentTestBed;
    
    protected Tenant mainTestTenant;

    @Resource(name = "modelRunService")
    protected ModelRunService modelRunService;

    protected void setTestBed(GlobalAuthDeploymentTestBed testBed) {
        this.deploymentTestBed = testBed;
    }
    
    protected void setupTestEnvironmentWithOneTenantForProduct(LatticeProduct product)
            throws NoSuchAlgorithmException, KeyManagementException, IOException {
        deploymentTestBed.bootstrapForProduct(product);
        mainTestTenant = deploymentTestBed.getMainTestTenant();
        switchToSuperAdmin();
    }

    protected void switchToSuperAdmin() {
        deploymentTestBed.switchToSuperAdmin(mainTestTenant);
    }

    protected List<ModelRun> createModelRuns() {
        List<SelectedConfig> selectedConfigs = getSelectedConfigs();
        List<ModelRun> modelRuns = new ArrayList<>();
        int i = 1;
        for (SelectedConfig selectedConfig : selectedConfigs) {
            ModelRun modelRun = new ModelRun();
            modelRun.setName("modelRun" + i++);
            modelRun.setStatus(ModelRunStatus.NEW);
            modelRun.setSelectedConfig(selectedConfig);
            modelRuns.add(modelRun);
        }
        

        return modelRuns;
    }

    protected ModelConfig createModelConfig(String algoName) {
        SelectedConfig selectedConfig = getSelectedConfigs().get(0);
        ModelConfig modelConfig = new ModelConfig();
        modelConfig.setName("modelConfig1");
        modelConfig.setSelectedConfig(selectedConfig);
        return modelConfig;
    }

    @SuppressWarnings("unchecked")
    protected List<SelectedConfig> getSelectedConfigs() {
        Algorithm algo = createAlgorithm();
        DataSet dataSet = createDataSet();
        DataFlow dataFlow = createDataFlow();
        PropData propData = createPropData();
        Pipeline pipeline = createPipeline();
        Sampling sampling = createSampling();

        SelectedConfig selectedConfig1 = new SelectedConfig();
        selectedConfig1.setAlgorithm(algo);
        selectedConfig1.setDataFlow(dataFlow);
        selectedConfig1.setDataSet(dataSet);
        selectedConfig1.setPipeline(pipeline);
        selectedConfig1.setPropData(propData);
        selectedConfig1.setSampling(sampling);

        SelectedConfig selectedConfig2 = new SelectedConfig();
        selectedConfig2.setAlgorithm(algo);
        selectedConfig2.setDataFlow(dataFlow);
        selectedConfig2.setDataSet(dataSet);
        selectedConfig2.setPipeline(createNewDataPipeline(pipeline));
        selectedConfig2.setPropData(propData);
        selectedConfig2.setSampling(sampling);
        
        return Arrays.asList(new SelectedConfig[] { selectedConfig1, selectedConfig2 });
    }
    
    protected String uploadPipelineStepFile(String extension) throws Exception {
        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();

        org.springframework.core.io.Resource resource = new ClassPathResource(
                "com/latticeengines/modelquality/service/impl/assignconversionratetoallcategoricalvalues." + extension);
        map.add("file", resource);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(map, headers);
        String fileName = "assignconversionratetoallcategoricalvalues" + extension;

        if (extension.equals("py")) {
            return modelQualityProxy.uploadPipelineStepPythonScript(fileName, "assigncategorical", requestEntity);
        } else if (extension.equals("json")) {
            return modelQualityProxy.uploadPipelineStepMetadata(fileName, "assigncategorical", requestEntity);
        }
        return null;
    }
    
    protected Pipeline createNewDataPipeline(Pipeline sourcePipeline) {
        try {
            uploadPipelineStepFile("py");
            uploadPipelineStepFile("json");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        List<PipelineStepOrFile> pipelineSteps = new ArrayList<>();
        
        for (PipelineStep step : sourcePipeline.getPipelineSteps()) {
            PipelineStepOrFile p = new PipelineStepOrFile();

            if (!step.getMainClassName().equals("EnumeratedColumnTransformStep")) {
                p.pipelineStepName = step.getName();
            } else {
                Path path = new Path(hdfsDir + "/steps/assigncategorical");
                p.pipelineStepDir = path.toString();
            }
            
            pipelineSteps.add(p);
        }
        String newPipelineName = modelQualityProxy.createPipeline("P1", pipelineSteps);
        return modelQualityProxy.getPipelineByName(newPipelineName);
    }

    protected Sampling createSampling() {
        return modelQualityProxy.createSamplingFromProduction();
    }

    protected DataFlow createDataFlow() {
        return modelQualityProxy.createDataFlowFromProduction();
    }

    protected Pipeline createPipeline() {
        return modelQualityProxy.createPipelineFromProduction();
    }

    protected PropData createPropData() {
        return modelQualityProxy.createPropDataConfigFromProduction();
    }

    protected DataSet createDataSet() {
        DataSet dataSet = new DataSet();
        dataSet.setName("DataSet1"); 
        dataSet.setIndustry("Industry1");
        dataSet.setTenant(new Tenant("Model_Quality_Test.Model_Quality_Test.Production"));
        dataSet.setDataSetType(DataSetType.FILE);
        dataSet.setSchemaInterpretation(SchemaInterpretation.SalesforceLead);
        dataSet.setTrainingSetHdfsPath("/Pods/Default/Services/ModelQuality/Mulesoft_MKTO_LP3_ScoringLead_20160316_170113.csv");
        ScoringDataSet scoringDataSet = new ScoringDataSet();
        scoringDataSet.setName("ScoringDataSet1");
        scoringDataSet.setDataHdfsPath("ScoringDataSetPath1");
        dataSet.addScoringDataSet(scoringDataSet);
        return dataSet;
    }

    protected Algorithm createAlgorithm() {
        return modelQualityProxy.createAlgorithmFromProduction();
    }

    protected void waitAndCheckModelRun(String modelRunId) {
        Assert.assertTrue(modelRunId != null && modelRunId != "");
        
        long start = System.currentTimeMillis();
        while (true) {
            ResponseDocument<ModelRun> result = modelQualityProxy.getModelRun(modelRunId);
            Assert.assertTrue(result.isSuccess(), "Failed for modelRunId=" + modelRunId);
            
            ModelRun modelRun = new ObjectMapper().convertValue(result.getResult(), ModelRun.class);
            if (modelRun.getStatus().equals(ModelRunStatus.COMPLETED)) {
                break;
            }
            if (modelRun.getStatus().equals(ModelRunStatus.FAILED)) {
                Assert.fail("Faield due to= " + modelRun.getErrorMessage());
                break;
            }
            System.out.println("Waiting for modelRunId=" + modelRunId + " Status:" + modelRun.getStatus().toString());
            long end = System.currentTimeMillis();
            if ((end - start) > 10 * 3_600_000) { // 10 hours max
                Assert.fail("Timeout for modelRunId=" + modelRunId);
            }
            try {
                Thread.sleep(300_000); // 5 mins
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

}
