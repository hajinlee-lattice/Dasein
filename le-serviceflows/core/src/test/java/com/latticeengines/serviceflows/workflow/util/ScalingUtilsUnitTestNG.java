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

}
