package com.latticeengines.domain.exposed.datacloud.dataflow;


import java.util.Arrays;
import java.util.List;

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
        Assert.assertEquals(bucket.generateLabels(), Arrays.asList(null, "< 0", "0 - 10", "10 - 100", ">= 100"));

        bucket = new IntervalBucket();
        bucket.setBoundaries(Arrays.asList(0.0, 10.0, 100.0));
        Assert.assertEquals(bucket.generateLabels(), Arrays.asList(null, "< 0", "0 - 10", "10 - 100", ">= 100"));

        bucket = new IntervalBucket();
        bucket.setBoundaries(Arrays.asList(0L, 1000L, 2000_000L, 3000_000_000L));
        Assert.assertEquals(bucket.generateLabels(), Arrays.asList(null, "< 0", "0 - 1K", "1K - 2M", "2M - 3B", ">= 3B"));
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

    @Test(groups = "unit")
    public void testDateBoundaries() {
        // Create a date bucket with an assumed current time of 10/23/2018 00:00:00 UTC.
        // Since the current time is right on the date boundary, no truncation occurs.
        DateBucket bucket = new DateBucket(1540252800000L);

        // Test that the correct date boundaries are set.
        List<Long> boundaries = bucket.getDateBoundaries();
        Assert.assertEquals(boundaries, Arrays.asList(
                1539734400000L,  // 10/17/2018 00:00:00 GMT - 6 days before
                1537747200000L,  // 09/24/2018 00:00:00 GMT - 29 days before
                1532563200000L,  // 07/26/2018 00:00:00 GMT - 89 days before
                1524787200000L   // 04/27/2018 00:00:00 GMT - 179 days before
        ));

        // Test that the buckets have the correct default labels.
        Assert.assertEquals(bucket.generateLabels(), Arrays.asList(
                null,
                "Last 7 Days",
                "Last 30 Days",
                "Last 90 Days",
                "Last 180 Days",
                "Ever"
        ));

        // Test setting the date bucket labels to custom values.
        bucket = new DateBucket(1540252800000L);
        bucket.setLast7DaysLabel("Last Week");
        bucket.setLast30DaysLabel("Last Month");
        bucket.setLast90DaysLabel("Last Quarter");
        bucket.setLast180DaysLabel("Last Two Quarters");
        bucket.setEverLabel("More Than Two Quarters");

        Assert.assertEquals(bucket.generateLabels(), Arrays.asList(
                null,
                "Last Week",
                "Last Month",
                "Last Quarter",
                "Last Two Quarters",
                "More Than Two Quarters"
        ));

        // Create a date bucket with an assumed current time of 10/23/2018 12:12:12 UTC.
        // This will test if truncation of hours, minutes, and seconds is working.
        bucket = new DateBucket(1540296732000L);

        // Test that the correct date boundaries are set.
        boundaries = bucket.getDateBoundaries();
        Assert.assertEquals(boundaries, Arrays.asList(
                1539734400000L,  // 10/17/2018 00:00:00 GMT - 6 days before
                1537747200000L,  // 09/24/2018 00:00:00 GMT - 29 days before
                1532563200000L,  // 07/26/2018 00:00:00 GMT - 89 days before
                1524787200000L   // 04/27/2018 00:00:00 GMT - 179 days before
        ));

    }
}
