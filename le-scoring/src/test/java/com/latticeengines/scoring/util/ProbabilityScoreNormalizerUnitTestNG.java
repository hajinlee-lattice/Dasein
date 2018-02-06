package com.latticeengines.scoring.util;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ProbabilityScoreNormalizerUnitTestNG {

    private ProbabilityScoreNormalizer normalizer = null;

    @BeforeClass(groups = "unit")
    public void beforeClass() throws Exception {

        List<NormalizationBucket> buckets = new ArrayList<>();
        buckets.add(new NormalizationBucket(0.43439529417124634, 0.47080762707065127, 0.009666666666666667));
        buckets.add(new NormalizationBucket(0.47080762707065127, 0.49022121569955224, 0.019333333333333334));
        buckets.add(new NormalizationBucket(0.49022121569955224, 0.509688515752666, 0.029));
        buckets.add(new NormalizationBucket(0.509688515752666, 0.5319679693950962, 0.03866666666666667));
        normalizer = new ProbabilityScoreNormalizer(buckets);
    }

    @Test(groups = "unit", dataProvider = "dataProvider")
    public void normalize(double input, double expected) {
        Assert.assertEquals(normalizer.getMinimumScore(), 5.0);
        Assert.assertEquals(normalizer.getMaximumScore(), 95.0);
        Assert.assertEquals(normalizer.getMinimumProbability(), 0.43439529417124634);
        Assert.assertEquals(normalizer.getMaximumProbability(), 0.5319679693950962);

        double actual = normalizer.normalize(input, InterpolationFunctionType.PositiveConvex);
        Assert.assertEquals(actual, expected);
    }

    @DataProvider(name = "dataProvider")
    public Object[][] dataProvider() {
        return new Object[][] { //
                new Object[] { 0.2, 1.7014782069336496 }, //
                new Object[] { 0.3, 2.8951667306145272 }, //
                new Object[] { 0.4, 4.397851245395385 }, //
                new Object[] { 0.5, 5.834561523524652 }, //
                new Object[] { 0.6, 95.45526026526282 }, //
                new Object[] { 0.7, 96.25684534480898 }, //
        };
    }
}
