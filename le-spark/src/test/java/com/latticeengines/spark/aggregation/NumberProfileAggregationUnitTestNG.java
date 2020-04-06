package com.latticeengines.spark.aggregation;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class NumberProfileAggregationUnitTestNG {

    @Test(groups = "functional", dataProvider = "roundToData")
    public void testRoundTo(double x, int sigDigits, double y) {
        Assert.assertEquals(NumberProfileAggregation.roundTo(x, sigDigits), y);
    }

    @DataProvider(name = "roundToData")
    public Object[][] providRoundToData() {
        return new Object[][]{
                { 0., 2, 0. },
                { 4.5, 1, 4. },
                { 1234., 1, 1000. },
                { -1640., 1, -2000. },
                { 1234., 2, 1200. },
                { -1264., 2, -1300. },
                { 0.1264, 2, 0.13 },
                { -0.1234, 2, -0.12 },
                { 0.001255, 2, 0.0013 },
        };
    }

    @Test(groups = "functional", dataProvider = "roundTo5Data")
    public void testRoundTo5(double x, double y) {
        Assert.assertEquals(NumberProfileAggregation.roundTo5(x), y);
    }

    @DataProvider(name = "roundTo5Data")
    public Object[][] providRoundTo5Data() {
        return new Object[][]{
                { 0., 0. },
                { 6.356, 6 },
                { 1234., 1000. },
                { 3500, 3500. },
                { 16345, 15000. },
                { -1264, -1500. },
                { 0.1264, 0.1 },
                { -0.1234, -0.1 },
                { 0.00155, 0.002 },
        };
    }

}
