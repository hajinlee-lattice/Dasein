package com.latticeengines.modelquality.controller;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;

public class PropDataResourceDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        propDataEntityMgr.deleteAll();
    }

    @Test(groups = "deployment")
    public void createPropDataConfigFromProduction() {
        PropData propDataConfig = modelQualityProxy.createPropDataConfigFromProduction();
        Assert.assertNotNull(propDataConfig);
    }

    @Test(groups = "deployment", dependsOnMethods = "createPropDataConfigFromProduction")
    public void getPropDatas() {
        List<PropData> configs = modelQualityProxy.getPropDataConfigs();
        Assert.assertEquals(configs.size(), 1);
    }
    
    // Ensure attempting to create Production again does not fail
    @Test(groups = "deployment", dependsOnMethods = "createPropDataConfigFromProduction")
    public void createPropDataConfigFromProductionAgain() {
        PropData propdata = modelQualityProxy.createPropDataConfigFromProduction();
        Assert.assertNotNull(propdata);
    }
}
