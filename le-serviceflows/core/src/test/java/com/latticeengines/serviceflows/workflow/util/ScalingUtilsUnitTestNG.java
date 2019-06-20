package com.latticeengines.serviceflows.workflow.util;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ScalingUtilsUnitTestNG {

    @Test(groups = "unit", dataProvider = "multiplierTestData")
    public void testMultiplier(double sizeInGb, int expectedMultiplier) {
        Assert.assertEquals(ScalingUtils.getMultiplier(sizeInGb), expectedMultiplier, String.format("%f", sizeInGb));
    }

    @DataProvider(name = "multiplierTestData")
    public Object[][] provideMultiplierTestData() {
        return new Object[][] { //
                { 0.1, 1 }, //
                { 1, 1 }, //
                { 7.9, 1 }, //
                { 8, 2 }, //
                { 8.1, 2 }, //
                { 9, 2 }, //
                { 23.9, 2 }, //
                { 24, 3 }, //
                { 24.0, 3 }, //
                { 50, 3 }, //
                { 72, 4 }, //
                { 500, 4 }, //
        };
    }

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
