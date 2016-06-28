package com.latticeengines.modelquality.controller;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;

public class PropDataResourceTestNG extends ModelQualityDeploymentTestNGBase {

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        propDataEntityMgr.deleteAll();
    }

    @Test(groups = "deployment")
    public void upsertPropDatas() {
        try {
            PropData propDatas = createPropData();
            ResponseDocument<String> response = modelQualityProxy.upsertPropDatas(Arrays.asList(propDatas));
            Assert.assertTrue(response.isSuccess());

        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test(groups = "deployment", dependsOnMethods = "upsertPropDatas")
    public void getPropDatas() {
        try {
            ResponseDocument<List<PropData>> response = modelQualityProxy.getPropDatas();
            Assert.assertTrue(response.isSuccess());
            Assert.assertEquals(response.getResult().size(), 1);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }
}
