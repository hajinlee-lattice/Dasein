package com.latticeengines.scoring.util;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ExpectedRevenueScoreNormalizerUnitTestNG {

    private ExpectedRevenueScoreNormalizer normalizer = null;

    @BeforeClass(groups = "unit")
    public void beforeClass() {

        List<NormalizationBucket> buckets = new ArrayList<>();
        buckets.add(new NormalizationBucket(260.5446797729171, 282.1205494105959, 0.008319467554076539));
        buckets.add(new NormalizationBucket(282.1205494105959, 312.43465088445544, 0.016638935108153077));
        buckets.add(new NormalizationBucket(312.43465088445544, 321.5833138521513, 0.024958402662229616));
        buckets.add(new NormalizationBucket(321.5833138521513, 329.51894249556096, 0.033277870216306155));
        normalizer = new ExpectedRevenueScoreNormalizer(buckets);
    }

    @Test(groups = "unit", dataProvider = "dataProvider")
    public void normalize(double input, double expected) {
        Assert.assertEquals(normalizer.getMinimumScore(), 5.0);
        Assert.assertEquals(normalizer.getMaximumScore(), 95.0);
        Assert.assertEquals(normalizer.getMinimumExpectedRevenue(), 260.5446797729171);
        Assert.assertEquals(normalizer.getMaximumExpectedRevenue(), 329.51894249556096);

        double actual = normalizer.normalize(input, InterpolationFunctionType.PositiveConvex);
        Assert.assertEquals(actual, expected);
    }

    @DataProvider(name = "dataProvider")
    public Object[][] dataProvider() {
        return new Object[][] { //
                new Object[] { 0.0, 0.0 }, //
                new Object[] { 200.0, 3.8381132973874954 }, //
                new Object[] { 280.0, 5.256844546984859 }, //
                new Object[] { 300.0, 5.454028928628629 }, //
                new Object[] { 400.0, 71.78126468536334 }, //
                new Object[] { 500.0, 79.16751723716564 }, //
        };
    }
}
