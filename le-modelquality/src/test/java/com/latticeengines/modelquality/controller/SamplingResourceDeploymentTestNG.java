package com.latticeengines.modelquality.controller;

import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.Sampling;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;

public class SamplingResourceDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        super.cleanupDb();
    }

    @Test(groups = "deployment")
    public void createSamplingFromProduction() {
        Sampling samplingConfig = modelQualityProxy.createSamplingFromProduction();
        Assert.assertNotNull(samplingConfig);
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "deployment", dependsOnMethods = "createSamplingFromProduction")
    public void getSamplingConfigs() {
        List<Sampling> samplingConfigs = modelQualityProxy.getSamplingConfigs();
        Assert.assertEquals(samplingConfigs.size(), 1);
        
        Sampling samplingConfig = modelQualityProxy.getSamplingConfigByName((String) ((Map) samplingConfigs.get(0)).get("name"));
        Assert.assertNotNull(samplingConfig);
    }
    
    // Ensure attempting to create Production again does not fail
    @Test(groups = "deployment", dependsOnMethods = "createSamplingFromProduction")
    public void createSamplingFromProductionAgain() {
        Sampling samplingConfig = modelQualityProxy.createSamplingFromProduction();
        Assert.assertNotNull(samplingConfig);
    }
}
