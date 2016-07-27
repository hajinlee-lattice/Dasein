package com.latticeengines.modelquality.controller;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.modelquality.Sampling;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;

public class SamplingResourceDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        samplingEntityMgr.deleteAll();
    }

    @Test(groups = "deployment")
    public void upsertSamplings() {
        try {
            Sampling samplings = createSampling();
            ResponseDocument<String> response = modelQualityProxy.upsertSamplings(Arrays.asList(samplings));
            Assert.assertTrue(response.isSuccess());

        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test(groups = "deployment", dependsOnMethods = "upsertSamplings")
    public void getSamplings() {
        try {
            ResponseDocument<List<Sampling>> response = modelQualityProxy.getSamplings();
            Assert.assertTrue(response.isSuccess());
            Assert.assertEquals(response.getResult().size(), 1);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }
}
