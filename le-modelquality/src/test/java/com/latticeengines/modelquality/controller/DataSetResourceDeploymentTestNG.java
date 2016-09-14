package com.latticeengines.modelquality.controller;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;

public class DataSetResourceDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        dataSetEntityMgr.deleteAll();
    }

    @Test(groups = "deployment")
    public void createDataset() {
        DataSet dataSet = super.createDataSet();
        String response = modelQualityProxy.createDataSet(dataSet);
        Assert.assertEquals(response, "DataSet1");
    }

    @Test(groups = "deployment", dependsOnMethods = "createDataset")
    public void getDataSets() {
        List<DataSet> dataSets = modelQualityProxy.getDataSets();
        Assert.assertEquals(dataSets.size(), 1);
    }
}
