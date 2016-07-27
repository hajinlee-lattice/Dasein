package com.latticeengines.modelquality.controller;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;

public class DataFlowResourceDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        dataFlowEntityMgr.deleteAll();
    }

    @Test(groups = "deployment")
    public void upsertDataFlows() {
        try {
            DataFlow dataFlows = createDataFlow();
            ResponseDocument<String> response = modelQualityProxy.upsertDataFlows(Arrays.asList(dataFlows));
            Assert.assertTrue(response.isSuccess());

        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test(groups = "deployment", dependsOnMethods = "upsertDataFlows")
    public void getDataFlows() {
        try {
            ResponseDocument<List<DataFlow>> response = modelQualityProxy.getDataFlows();
            Assert.assertTrue(response.isSuccess());
            Assert.assertEquals(response.getResult().size(), 1);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }
}
