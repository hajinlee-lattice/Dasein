package com.latticeengines.dataflow.runtime.cascading.propdata;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.dataflow.DateBucket;

public class BucketEncodeFunctionUnitTestNG {

    @Test(groups = "unit")
    public void testBucketDate() {

        DateBucket dateBucket = new DateBucket(1550000000000L);
        // Null value.
        Assert.assertEquals(BucketEncodeFunction.bucketDate(null, dateBucket), 0);
        // Negative value
        Assert.assertEquals(BucketEncodeFunction.bucketDate(-1550000000000L, dateBucket), 0);
        // Current time.
        Assert.assertEquals(BucketEncodeFunction.bucketDate(1550000000000L, dateBucket), 1);
        // Between current time and last 7 days.
        Assert.assertEquals(BucketEncodeFunction.bucketDate(1549900000000L, dateBucket), 1);
        // Between last 7 days and last 30 days.
        Assert.assertEquals(BucketEncodeFunction.bucketDate(1549000000000L, dateBucket), 2);
        // Between last 30 days and last 90 days.
        Assert.assertEquals(BucketEncodeFunction.bucketDate(1544000000000L, dateBucket), 3);
        // Between last 90 days and last 180 days.
        Assert.assertEquals(BucketEncodeFunction.bucketDate(1540000000000L, dateBucket), 4);
        // Greater than last 180 days.
        Assert.assertEquals(BucketEncodeFunction.bucketDate(1500000000000L, dateBucket), 5);
        // Future value.
        Assert.assertEquals(BucketEncodeFunction.bucketDate(1551000000000L, dateBucket), 5);
        // String value between current time and last 7 days.
        Assert.assertEquals(BucketEncodeFunction.bucketDate("1549910000000", dateBucket), 1);
        // Unparsable string value.
        Assert.assertEquals(BucketEncodeFunction.bucketDate("abcdefghijklm", dateBucket), 0);
    }
}
