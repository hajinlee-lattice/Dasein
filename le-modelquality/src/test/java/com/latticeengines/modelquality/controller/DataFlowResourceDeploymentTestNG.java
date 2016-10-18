package com.latticeengines.modelquality.controller;

import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;

public class DataFlowResourceDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        super.cleanupDb();
    }

    @Test(groups = "deployment")
    public void createDataFlowFromProduction() {
        DataFlow dataFlow = modelQualityProxy.createDataFlowFromProduction();
        Assert.assertNotNull(dataFlow);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment", dependsOnMethods = "createDataFlowFromProduction")
    public void getDataFlows() {
        List<DataFlow> dataFlows = modelQualityProxy.getDataFlows();
        Assert.assertEquals(dataFlows.size(), 1);
        String name = ((Map<String, String>) dataFlows.get(0)).get("name");
        Assert.assertTrue(name.startsWith("PRODUCTION-"));
    }

}
