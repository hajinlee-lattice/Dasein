package com.latticeengines.dataflow.runtime.cascading.cdl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.stream.DoubleStream;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SimplePercentileCalculatorUnitTestNG {

    @Test(groups = "unit")
    public void testSmallDataSet() {
        Double[] smallDataSet = { 0.50, 0.35, 0.15, 0.02, 0.01 };
        SimplePercentileCalculator percentileCalculator = new SimplePercentileCalculator(5, 99);
        int total = smallDataSet.length;
        for (int i = 0; i < total; ++i) {
            percentileCalculator.compute(total, i, smallDataSet[i]);
        }
        percentileCalculator.adjustBoundaries();
        assertEquals(5, percentileCalculator.getMinPct());
        assertEquals(99, percentileCalculator.getMaxPct());
        Assert.assertNotNull(percentileCalculator.getLowerBound(5));
        Assert.assertNotNull(percentileCalculator.getUpperBound(5));
        Assert.assertNotNull(percentileCalculator.getLowerBound(99));
        Assert.assertNotNull(percentileCalculator.getUpperBound(99));

        verifyResults(percentileCalculator);
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void testDataWithWrongOrder() {
        Double[] smallDataSet = { 0.01, 0.35, 0.15, 0.02, 0.01 };
        SimplePercentileCalculator percentileCalculator = new SimplePercentileCalculator(5, 99);
        int total = smallDataSet.length;
        for (int i = 0; i < total; ++i) {
            percentileCalculator.compute(total, i, smallDataSet[i]);
        }
        percentileCalculator.adjustBoundaries();
    }

    @Test(groups = "unit", dataProvider = "dataSetProvider")
    public void testWithRandomGeneratedDataSet(Double[] testDataSet) {
        SimplePercentileCalculator percentileCalculator = new SimplePercentileCalculator(5, 99);
        int total = testDataSet.length;
        for (int i = 0; i < total; ++i) {
            percentileCalculator.compute(total, i, testDataSet[i]);
        }
        percentileCalculator.adjustBoundaries();
        assertEquals(5, percentileCalculator.getMinPct());
        assertEquals(99, percentileCalculator.getMaxPct());
        Assert.assertNotNull(percentileCalculator.getLowerBound(5));
        Assert.assertNotNull(percentileCalculator.getUpperBound(5));
        Assert.assertNotNull(percentileCalculator.getLowerBound(99));
        Assert.assertNotNull(percentileCalculator.getUpperBound(99));

        verifyResults(percentileCalculator);
    }

    @DataProvider(name = "dataSetProvider", parallel = true)
    public Object[][] dataSetProvider() {
        return new Object[][] { //
                generateSortedRandomDataSet(5), //
                generateSortedRandomDataSet(250), //
                generateSortedRandomDataSet(1250) //
        };
    }

    private Double[] generateSortedRandomDataSet(int n) {
        Random random = new Random();
        DoubleStream ds = random.doubles(n);
        Double[] randomData = ds.boxed().toArray(Double[]::new);
        Arrays.sort(randomData, Comparator.reverseOrder());
        return randomData;
    }

    private void verifyResults(SimplePercentileCalculator calculator) {
        Double initValue = -0.00001;
        Double prevLower = initValue;
        Double prevUpper = initValue;

        for (int pct = calculator.getMinPct(); pct <= calculator.getMaxPct(); ++pct) {

            Double curLower = calculator.getLowerBound(pct);
            Double curUpper = calculator.getUpperBound(pct);
            assertTrue(curLower > prevLower);
            assertTrue(curUpper > prevUpper);

            if (!prevUpper.equals(initValue)) {
                assertEquals(curLower, prevUpper, //
                        "Current lower bound " + curLower + "is not equal to previous upper bound " + prevUpper);
            }
            assertTrue(curLower >= 0 && curLower <= 1.);
            assertTrue(curUpper >= 0 && curUpper <= 1.);
            assertTrue(curLower < curUpper, //
                    "Lower bound " + curLower + " is larger than or equal to upper bound " + curUpper);
            prevLower = curLower;
            prevUpper = curUpper;
        }
    }
}
