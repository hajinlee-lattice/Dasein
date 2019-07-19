package com.latticeengines.serviceflows.workflow.util;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ScalingUtilsUnitTestNG {

    @Test(groups = "unit", dataProvider = "dataflowScalingTestData")
    public void testScaleDataflowAm(int numModels, int expectedVCores, int expectedMemGb) {
        Assert.assertEquals(ScalingUtils.scaleDataFlowAmVCoresByNumModels(numModels), expectedVCores);
        Assert.assertEquals(ScalingUtils.scaleDataFlowAmMemGbByNumModels(numModels), expectedMemGb);
    }

    @DataProvider(name = "dataflowScalingTestData")
    public Object[][] provideDataflowScalingTestData() {
        return new Object[][] { //
                { 1, 1, 4 }, //
                { 8, 1, 4 }, //
                { 9, 2, 8 }, //
                { 16, 2, 8 }, //
                { 17, 3, 12 }, //
                { 40, 5, 20 }, //
                { 41, 6, 24 }, //
                { 48, 6, 24 }, //
                { 98, 6, 24 }, //
        };
    }

}
