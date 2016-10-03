package com.latticeengines.modelquality.controller;

import org.testng.annotations.BeforeClass;

import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;

public class ModelConfigResourceDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        modelConfigEntityMgr.deleteAll();
    }

}
