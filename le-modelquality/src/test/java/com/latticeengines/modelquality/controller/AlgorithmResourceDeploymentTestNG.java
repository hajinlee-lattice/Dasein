package com.latticeengines.modelquality.controller;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.Algorithm;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;

public class AlgorithmResourceDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        // algorithmEntityMgr.deleteAll();
    	super.cleanupDb();
    }

    @Test(groups = "deployment")
    public void createAlgorithmFromProduction() {
        Algorithm algorithm = modelQualityProxy.createAlgorithmFromProduction();
        Assert.assertNotNull(algorithm);
    }

    @Test(groups = "deployment", dependsOnMethods = "createAlgorithmFromProduction")
    public void getAlgorithms() {
        List<Algorithm> algorithms = modelQualityProxy.getAlgorithms();
        Assert.assertEquals(algorithms.size(), 1);
    }
}
