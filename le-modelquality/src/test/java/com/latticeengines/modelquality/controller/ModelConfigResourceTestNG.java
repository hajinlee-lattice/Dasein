package com.latticeengines.modelquality.controller;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.modeling.factory.AlgorithmFactory;
import com.latticeengines.domain.exposed.modelquality.ModelConfig;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;

public class ModelConfigResourceTestNG extends ModelQualityDeploymentTestNGBase {

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        modelConfigEntityMgr.deleteAll();
    }

    @Test(groups = "deployment")
    public void upsertModelConfig() {
        try {
            ModelConfig modelConfig = createModelConfig(AlgorithmFactory.ALGORITHM_NAME_RF);
            ResponseDocument<String> response = modelQualityProxy.upsertModelConfigs(Arrays.asList(modelConfig));
            Assert.assertTrue(response.isSuccess());

        } catch (Exception ex) {
            Assert.fail("Failed on upsertModelConfig", ex);
        }
    }

    @Test(groups = "deployment", dependsOnMethods = "upsertModelConfig")
    public void getModelConfigs() {
        try {
            ResponseDocument<List<ModelConfig>> response = modelQualityProxy.getModelConfigs();
            Assert.assertTrue(response.isSuccess());
            Assert.assertEquals(response.getResult().size(), 1);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }
}
