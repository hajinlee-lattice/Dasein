package com.latticeengines.spark.util;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.spark.DeleteUtils;

import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

public class DeleteUtilsUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(DeleteUtilsUnitTestNG.class);

    @Test(groups = "unit", dataProvider = "timeRangeSerDe")
    private void testTimeRangeSerialization(long[][] timeRanges) {
        // test time range serialization
        List<WrappedArray<Object>> ranges = Arrays.stream(timeRanges).map(WrappedArray::make)
                .collect(Collectors.toList());
        String rangeStr = DeleteUtils.serializeTimeRanges(JavaConversions.asScalaBuffer(ranges));
        Assert.assertNotNull(rangeStr);

        log.info("Time ranges {} is serialized into string {}", Arrays.deepToString(timeRanges), rangeStr);

        // deserialize time range strings back
        Option<long[][]> deserializedRanges = DeleteUtils.deserializeTimeRanges(rangeStr);
        Assert.assertNotNull(deserializedRanges);
        Assert.assertTrue(deserializedRanges.isDefined(), String.format("should be able to deserialize time range string %s", rangeStr));
        long[][] resRanges = deserializedRanges.get();
        Assert.assertEquals(resRanges.length, timeRanges.length);
        IntStream.range(0, resRanges.length).forEach(idx -> {
            Assert.assertEquals(resRanges[idx], timeRanges[idx]);
        });
    }

    @DataProvider(name = "timeRangeSerDe")
    private Object[][] timeRangeSerializationTestData() { //
        return new Object[][] { //
                { new long[][] { { 0L, 1L } } }, //
                { new long[][] { { 0L, 1L }, { Long.MIN_VALUE, Long.MAX_VALUE } } }, //
                { new long[][] { { Long.MIN_VALUE, Long.MAX_VALUE } } }, //
                { new long[][] { { 0L, 1L }, { 100L, 155L } } }, //
                { new long[][] { { 0L, 1L }, { 10L, 11L }, { 105L, 5000000L } } }, //
        }; //
    }
}
