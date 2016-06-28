package com.latticeengines.modelquality.controller;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.modeling.factory.AlgorithmFactory;
import com.latticeengines.domain.exposed.modelquality.Algorithm;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;

public class AlgorithmResourceTestNG extends ModelQualityDeploymentTestNGBase {

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        algorithmEntityMgr.deleteAll();
    }

    @Test(groups = "deployment")
    public void upsertAlgorithms() {
        try {
            Algorithm algorithms = createAlgorithm(AlgorithmFactory.ALGORITHM_NAME_RF);
            ResponseDocument<String> response = modelQualityProxy.upsertAlgorithms(Arrays.asList(algorithms));
            Assert.assertTrue(response.isSuccess());

        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test(groups = "deployment", dependsOnMethods = "upsertAlgorithms")
    public void getAlgorithms() {
        try {
            ResponseDocument<List<Algorithm>> response = modelQualityProxy.getAlgorithms();
            Assert.assertTrue(response.isSuccess());
            Assert.assertEquals(response.getResult().size(), 1);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }
}
