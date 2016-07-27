package com.latticeengines.modelquality.controller;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;

public class DataSetResourceDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        dataSetEntityMgr.deleteAll();
    }

    @Test(groups = "deployment")
    public void upsertDataSets() {
        try {
            DataSet dataSets = createDataSet();
            ResponseDocument<String> response = modelQualityProxy.upsertDataSets(Arrays.asList(dataSets));
            Assert.assertTrue(response.isSuccess());

        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test(groups = "deployment", dependsOnMethods = "upsertDataSets")
    public void getDataSets() {
        try {
            ResponseDocument<List<DataSet>> response = modelQualityProxy.getDataSets();
            Assert.assertTrue(response.isSuccess());
            Assert.assertEquals(response.getResult().size(), 1);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }
}
