package com.latticeengines.serviceflows.workflow.util;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ScalingUtilsUnitTestNG {

    @Test(groups = "unit", dataProvider = "multiplierTestData")
    public void testMultiplier(long count, int expectedMultiplier) {
        Assert.assertEquals(ScalingUtils.getMultiplier(count), expectedMultiplier, String.format("%d", count));
    }

    @DataProvider(name = "multiplierTestData")
    public Object[][] provideMultiplierTestData() {
        return new Object[][] { //
                { 1000, 1 }, //
                { 100_000, 2 }, //
                { 100_001, 2 }, //
                { 550_000, 2 }, //
                { 1_000_000, 3 }, //
                { 10_000_000, 4 }, //
                { 15_000_000, 4 }, //
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
