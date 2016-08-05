package com.latticeengines.modelquality.controller;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.modeling.factory.AlgorithmFactory;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class ModelRunResourceDeploymentTestNG extends ModelQualityDeploymentTestNGBase {
    
    private String user = TestFrameworkUtils.usernameForAccessLevel(AccessLevel.SUPER_ADMIN);
    private String password = TestFrameworkUtils.GENERAL_PASSWORD;
    
    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        modelRunEntityMgr.deleteAll();
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
    }

    @Test(groups = "deployment")
    public void runModelMuleSoft() {
        try {
            ModelRun modelRun = createModelRun(AlgorithmFactory.ALGORITHM_NAME_RF);
            modelRun.getSelectedConfig().getDataSet().setName("MuleSoft");
            modelRun.getSelectedConfig() //
                    .getDataSet() //
                    .setTrainingSetHdfsPath( //
                            "/Pods/Default/Services/ModelQuality/Mulesoft_Migration_LP3_ModelingLead_ReducedRows_20160624_155355.csv");
            
            ResponseDocument<String> response = modelQualityProxy.runModel(modelRun, //
                    mainTestTenant.getId(), user, password, plsDeployedHostPort);
            Assert.assertTrue(response.isSuccess());
            
            String modelRunId = response.getResult();
            waitAndCheckModelRun(modelRunId);
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("Failed", ex);
        }
    }

    @Test(groups = "deployment", dependsOnMethods = "runModelMuleSoft", enabled = false)
    public void runModelAlfresco() {
        try {
            ModelRun modelRun = createModelRun(AlgorithmFactory.ALGORITHM_NAME_RF);
            modelRun.getSelectedConfig().getDataSet().setName("Alfresco");
            modelRun.getSelectedConfig()
                    .getDataSet()
                    .setTrainingSetHdfsPath(
                            "/Pods/Default/Services/ModelQuality/Alfresco_SFDC_LP3_ModelingLead_ReducedRows_20160712_125241.csv");
            ResponseDocument<String> response = modelQualityProxy.runModel(modelRun, //
                    mainTestTenant.getId(), user, password, plsDeployedHostPort);

            Assert.assertTrue(response.isSuccess());

            String modelRunId = response.getResult();
            waitAndCheckModelRun(modelRunId);

        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("Failed", ex);
        }
    }

    @Test(groups = "deployment", dependsOnMethods = "runModelAlfresco", enabled = false)
    public void runModelNGINX() {
        try {
            ModelRun modelRun = createModelRun(AlgorithmFactory.ALGORITHM_NAME_RF);
            modelRun.getSelectedConfig().getDataSet().setName("NGINX");
            modelRun.getSelectedConfig()
                    .getDataSet()
                    .setTrainingSetHdfsPath(
                            "/Pods/Default/Services/ModelQuality/NGINX_PLS_LP3_ModelingLead_ReducedRows_20160712_125224.csv");
            ResponseDocument<String> response = modelQualityProxy.runModel(modelRun, //
                    mainTestTenant.getId(), user, password, plsDeployedHostPort);

            Assert.assertTrue(response.isSuccess());
            
            String modelRunId = response.getResult();
            waitAndCheckModelRun(modelRunId);

        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("Failed", ex);
        }
    }

    @Test(groups = "deployment", dependsOnMethods = "runModelNGINX", enabled = false)
    public void getModelRuns() {
        try {
            ResponseDocument<List<ModelRun>> response = modelQualityProxy.getModelRuns();
            Assert.assertTrue(response.isSuccess());
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }

    @Test(groups = "deployment", dependsOnMethods = "getModelRuns", enabled = false)
    public void deleteModelRuns() {
        try {
            modelQualityProxy.deleteModelRuns();
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }


}
