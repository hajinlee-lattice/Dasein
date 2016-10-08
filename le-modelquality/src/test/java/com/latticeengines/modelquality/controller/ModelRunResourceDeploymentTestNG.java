package com.latticeengines.modelquality.controller;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.ModelRunEntityNames;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class ModelRunResourceDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    private String user = TestFrameworkUtils.usernameForAccessLevel(AccessLevel.SUPER_ADMIN);
    private String password = TestFrameworkUtils.GENERAL_PASSWORD;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        super.cleanupDb();
        super.cleanupHdfs();
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
    }

    @Test(groups = "deployment")
    public void runModelMuleSoft() {
        try {
            List<ModelRun> modelRuns = createModelRuns();
            for (ModelRun modelRun : modelRuns) {
                modelRun.getDataSet().setName("MuleSoft");
                modelRun.getDataSet() //
                        .setTrainingSetHdfsPath( //
                                "/Pods/Default/Services/ModelQuality/Mulesoft_Migration_LP3_ModelingLead_ReducedRows_20160624_155355.csv");
                dataSetEntityMgr.create(modelRun.getDataSet());
                ModelRunEntityNames modelRunEntityNames = new ModelRunEntityNames(modelRun);

                String modelName = modelQualityProxy.createModelRun(modelRunEntityNames, //
                        mainTestTenant.getId(), user, password, plsDeployedHostPort);
                Assert.assertEquals(modelRun.getName(), modelName);
                waitAndCheckModelRun(modelName);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("Failed", ex);
        }
    }

    @Test(groups = "deployment", dependsOnMethods = "runModelMuleSoft", enabled = false)
    public void runModelAlfresco() {
        try {
            ModelRun modelRun = createModelRuns().get(0);
            modelRun.getDataSet().setName("Alfresco");
            modelRun.getDataSet().setTrainingSetHdfsPath(
                    "/Pods/Default/Services/ModelQuality/Alfresco_SFDC_LP3_ModelingLead_ReducedRows_20160712_125241.csv");
            dataSetEntityMgr.create(modelRun.getDataSet());
            ModelRunEntityNames modelRunEntityNames = new ModelRunEntityNames(modelRun);

            String modelName = modelQualityProxy.createModelRun(modelRunEntityNames, //
                    mainTestTenant.getId(), user, password, plsDeployedHostPort);
            Assert.assertEquals(modelRun.getName(), modelName);
            waitAndCheckModelRun(modelName);

        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("Failed", ex);
        }
    }

    @Test(groups = "deployment", dependsOnMethods = "runModelAlfresco", enabled = false)
    public void runModelNGINX() {
        try {
            ModelRun modelRun = createModelRuns().get(0);
            modelRun.getDataSet().setName("NGINX");
            modelRun.getDataSet().setTrainingSetHdfsPath(
                    "/Pods/Default/Services/ModelQuality/NGINX_PLS_LP3_ModelingLead_ReducedRows_20160712_125224.csv");
            dataSetEntityMgr.create(modelRun.getDataSet());
            ModelRunEntityNames modelRunEntityNames = new ModelRunEntityNames(modelRun);

            String modelName = modelQualityProxy.createModelRun(modelRunEntityNames, //
                    mainTestTenant.getId(), user, password, plsDeployedHostPort);
            Assert.assertEquals(modelRun.getName(), modelName);
            waitAndCheckModelRun(modelName);

        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("Failed", ex);
        }
    }

    @Test(groups = "deployment", dependsOnMethods = "runModelMuleSoft", enabled = true)
    public void getModelRuns() {
        try {
            List<ModelRunEntityNames> response = modelQualityProxy.getModelRuns();
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }

}
