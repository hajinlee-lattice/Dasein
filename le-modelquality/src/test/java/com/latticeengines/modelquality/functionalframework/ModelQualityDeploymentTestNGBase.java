package com.latticeengines.modelquality.functionalframework;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.domain.exposed.modelquality.Sampling;
import com.latticeengines.domain.exposed.modelquality.ScoringDataSet;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.modelquality.service.ModelRunService;
import com.latticeengines.proxy.exposed.modelquality.ModelQualityProxy;
import com.latticeengines.testframework.security.impl.GlobalAuthDeploymentTestBed;

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

    protected ModelRun createModelRun(String algoName) {
        SelectedConfig selectedConfig = getSelectedConfig(algoName);
        ModelRun modelRun = new ModelRun();
        modelRun.setName("modelRun1");
        modelRun.setStatus(ModelRunStatus.NEW);
        modelRun.setSelectedConfig(selectedConfig);

        return modelRun;
    }

    protected ModelConfig createModelConfig(String algoName) {
        SelectedConfig selectedConfig = getSelectedConfig(algoName);
        ModelConfig modelConfig = new ModelConfig();
        modelConfig.setName("modelConfig1");
        modelConfig.setSelectedConfig(selectedConfig);
        return modelConfig;
    }

    protected SelectedConfig getSelectedConfig(String algoName) {
        Algorithm algo = createAlgorithm();
        DataSet dataSet = createDataSet();
        DataFlow dataFlow = createDataFlow();
        PropData propData = createPropData();
        Pipeline pipeline = createPipeline();
        Sampling sampling = createSampling();

        SelectedConfig selectedConfig = new SelectedConfig();
        selectedConfig.setAlgorithm(algo);
        selectedConfig.setDataFlow(dataFlow);
        selectedConfig.setDataSet(dataSet);
        selectedConfig.setPipeline(pipeline);
        selectedConfig.setPropData(propData);
        selectedConfig.setSampling(sampling);
        return selectedConfig;
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
