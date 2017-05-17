package com.latticeengines.domain.exposed.datacloud.dataflow;


import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class BucketAlgorithmUnitTestNG {

    @Test(groups = "unit")
    public void test() {
        CategoricalBucket cBucket = new CategoricalBucket();
        IntervalBucket iBucket = new IntervalBucket();

        String cBktSer = JsonUtils.serialize(cBucket);
        String iBktSer = JsonUtils.serialize(iBucket);

        BucketAlgorithm algo = JsonUtils.deserialize(cBktSer, BucketAlgorithm.class);
        Assert.assertTrue(algo instanceof CategoricalBucket);
        algo = JsonUtils.deserialize(iBktSer, BucketAlgorithm.class);
        Assert.assertTrue(algo instanceof IntervalBucket);
    }

    @Test(groups = "unit")
    public void testIntervalLabels() {
        IntervalBucket bucket = new IntervalBucket();
        bucket.setBoundaries(Arrays.asList(0, 10, 100));
        Assert.assertEquals(bucket.generateLabels(), Arrays.asList(null, "< 0", "0 - 10", "10 - 100", "> 100"));

        bucket = new IntervalBucket();
        bucket.setBoundaries(Arrays.asList(0.0, 10.0, 100.0));
        Assert.assertEquals(bucket.generateLabels(), Arrays.asList(null, "< 0", "0 - 10", "10 - 100", "> 100"));

        bucket = new IntervalBucket();
        bucket.setBoundaries(Arrays.asList(0L, 1000L, 2000_000L, 3000_000_000L));
        Assert.assertEquals(bucket.generateLabels(), Arrays.asList(null, "< 0", "0 - 1K", "1K - 2M", "2M - 3B", "> 3B"));
    }

    @Test(groups = "unit")
    public void testCategoricalLabels() {
        CategoricalBucket bucket = new CategoricalBucket();
        bucket.setCategories(Arrays.asList("Cat1", "Cat2", "Cat3"));
        Assert.assertEquals(bucket.generateLabels(), Arrays.asList(null, "Cat1", "Cat2", "Cat3"));
    }

    @Test(groups = "unit")
    public void testBooleanLabels() {
        BooleanBucket bucket = new BooleanBucket();
        Assert.assertEquals(bucket.generateLabels(), Arrays.asList(null, "Yes", "No"));

        bucket = new BooleanBucket();
        bucket.setTrueLabel("T");
        bucket.setFalseLabel("F");
        Assert.assertEquals(bucket.generateLabels(), Arrays.asList(null, "T", "F"));
    }

}
