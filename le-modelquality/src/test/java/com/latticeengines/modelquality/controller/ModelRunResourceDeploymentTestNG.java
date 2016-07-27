package com.latticeengines.modelquality.controller;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.modeling.factory.AlgorithmFactory;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;

public class ModelRunResourceDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        modelRunEntityMgr.deleteAll();
    }

    @Test(groups = "deployment")
    public void runModel() {
        try {
            ModelRun modelRun = createModelRun(AlgorithmFactory.ALGORITHM_NAME_RF);
            ResponseDocument<String> response = modelQualityProxy.runModel(modelRun);
            Assert.assertTrue(response.isSuccess());

        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("Failed", ex);
        }
    }

    @Test(groups = "deployment", dependsOnMethods = "runModel")
    public void getModelRuns() {
        try {
            ResponseDocument<List<ModelRun>> response = modelQualityProxy.getModelRuns();
            Assert.assertTrue(response.isSuccess());
            Assert.assertEquals(response.getResult().size(), 1);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }

    @Test(groups = "deployment", dependsOnMethods = "getModelRuns")
    public void deleteModelRuns() {
        try {
            modelQualityProxy.deleteModelRuns();
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }
}
