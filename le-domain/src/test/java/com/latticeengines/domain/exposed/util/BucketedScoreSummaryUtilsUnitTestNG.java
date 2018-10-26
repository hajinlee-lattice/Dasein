package com.latticeengines.domain.exposed.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.BucketedScore;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;

public class BucketedScoreSummaryUtilsUnitTestNG {

    private static final String RESOURCE_ROOT = "com/latticeengines/domain/exposed" //
            + "/util/BucketedScoreSummaryUtilsUnitTestNG/";

    @Test(groups = "unit")
    public void testParseBucketedScore() throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream is = classLoader.getResourceAsStream(RESOURCE_ROOT + "part-00000.avro");
        List<GenericRecord> recordList = AvroUtils.readFromInputStream(is);
        BucketedScoreSummary summary = BucketedScoreSummaryUtils.generateBucketedScoreSummary(recordList, false);
        Assert.assertEquals(summary.getBarLifts().length, 32);
        List<BucketedScore> notNullBuckets = Arrays.stream(summary.getBucketedScores()).filter(Objects::nonNull)
                .collect(Collectors.toList());
        Assert.assertEquals(notNullBuckets.size(), 96);
        Assert.assertEquals(notNullBuckets.stream().map(BucketedScore::getNumLeads).reduce(0, (a, b) -> a + b),
                new Integer(summary.getTotalNumLeads()));
        notNullBuckets.stream().forEach(bucket -> {
            Assert.assertNull(bucket.getAverageExpectedRevenue());
            Assert.assertNull(bucket.getExpectedRevenue());
            Assert.assertNull(bucket.getLeftExpectedRevenue());
            Assert.assertNotNull(bucket.getLeftNumConverted());
            Assert.assertNotNull(bucket.getNumConverted());
            Assert.assertNotNull(bucket.getNumLeads());
            Assert.assertNotNull(bucket.getScore());
            if (bucket.getScore() == 99) {
                Assert.assertEquals(bucket.getLeftNumConverted(), 0D);
            } else {
                Assert.assertTrue(bucket.getLeftNumConverted() > 0D);
            }
            Assert.assertEquals(summary.getTotalNumLeads(), 4160);
            Assert.assertEquals(summary.getTotalNumConverted(), 72.59143547);
            Assert.assertEquals(summary.getOverallLift(), 0.017449864295673075);
            Assert.assertNull(summary.getTotalExpectedRevenue());
        });
    }

    @Test(groups = "unit")
    public void testParseBucketedScoreEV() throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream is = classLoader.getResourceAsStream(RESOURCE_ROOT + "ev-part-00000.avro");
        List<GenericRecord> recordList = AvroUtils.readFromInputStream(is);
        BucketedScoreSummary summary = BucketedScoreSummaryUtils.generateBucketedScoreSummary(recordList, true);
        Assert.assertEquals(summary.getBarLifts().length, 32);
        List<BucketedScore> notNullBuckets = Arrays.stream(summary.getBucketedScores()).filter(Objects::nonNull)
                .collect(Collectors.toList());
        Assert.assertEquals(notNullBuckets.size(), 96);
        Assert.assertEquals(notNullBuckets.stream().map(BucketedScore::getNumLeads).reduce(0, (a, b) -> a + b),
                new Integer(summary.getTotalNumLeads()));
        notNullBuckets.stream().forEach(bucket -> {
            Assert.assertNotNull(bucket.getAverageExpectedRevenue());
            Assert.assertNotNull(bucket.getExpectedRevenue());
            Assert.assertNotNull(bucket.getLeftExpectedRevenue());
            Assert.assertNotNull(bucket.getLeftNumConverted());
            Assert.assertNotNull(bucket.getNumConverted());
            Assert.assertNotNull(bucket.getNumLeads());
            Assert.assertNotNull(bucket.getScore());
            if (bucket.getScore() == 99) {
                Assert.assertEquals(bucket.getLeftExpectedRevenue(), 0D);
                Assert.assertEquals(bucket.getLeftNumConverted(), 0D);
            } else {
                Assert.assertTrue(bucket.getLeftExpectedRevenue() > 0D);
                Assert.assertTrue(bucket.getLeftNumConverted() > 0D);
            }
        });
        Assert.assertEquals(summary.getTotalNumLeads(), 9465);
        Assert.assertEquals(summary.getTotalNumConverted(), 94.71663841982246);
        Assert.assertEquals(summary.getOverallLift(), 0.010007040509225828);
        Assert.assertEquals(summary.getTotalExpectedRevenue(), 364241.36234563857);
    }

    @Test(groups = "unit")
    public void testComputeLift() throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream is = classLoader.getResourceAsStream(RESOURCE_ROOT + "bucketed_score_summary.json");
        ObjectMapper om = new ObjectMapper();
        BucketedScoreSummary summary = om.readValue(is, BucketedScoreSummary.class);
        List<BucketMetadata> bucketMetadataList = getBucketMetadata();
        bucketMetadataList = BucketedScoreSummaryUtils.computeLift(summary, bucketMetadataList, false);
        for (BucketMetadata bucketMetadata : bucketMetadataList) {
            String bucketName = bucketMetadata.getBucketName();
            double lift = Math.round(bucketMetadata.getLift() * 100) * 0.01;
            switch (bucketName) {
            case "A":
                Assert.assertEquals(lift, 5.34, JsonUtils.serialize(bucketMetadata));
                break;
            case "B":
                Assert.assertEquals(lift, 3.18, JsonUtils.serialize(bucketMetadata));
                break;
            case "C":
                Assert.assertEquals(lift, 1.47, JsonUtils.serialize(bucketMetadata));
                break;
            case "D":
                Assert.assertEquals(lift, 0.04, JsonUtils.serialize(bucketMetadata));
                break;
            }
        }
        Integer sumCount = bucketMetadataList.stream().map(BucketMetadata::getNumLeads).reduce(0, (a, b) -> a + b);
        Assert.assertEquals(sumCount, new Integer(summary.getTotalNumLeads()));
    }

    @Test(groups = "unit")
    public void testComputeLiftEV() throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream is = classLoader.getResourceAsStream(RESOURCE_ROOT + "bucketed_score_summary_ev.json");
        ObjectMapper om = new ObjectMapper();
        BucketedScoreSummary summary = om.readValue(is, BucketedScoreSummary.class);
        List<BucketMetadata> bucketMetadataList = getBucketMetadata();
        bucketMetadataList = BucketedScoreSummaryUtils.computeLift(summary, bucketMetadataList, true);
        for (BucketMetadata bucketMetadata : bucketMetadataList) {
            String bucketName = bucketMetadata.getBucketName();
            double lift = Math.round(bucketMetadata.getLift() * 100) * 0.01;
            switch (bucketName) {
            case "A":
                Assert.assertEquals(lift, 10.21, JsonUtils.serialize(bucketMetadata));
                Assert.assertEquals(bucketMetadata.getAverageExpectedRevenue(), 392.9047703147898,
                        JsonUtils.serialize(bucketMetadata));
                Assert.assertEquals(bucketMetadata.getTotalExpectedRevenue(), 223169.9095388006,
                        JsonUtils.serialize(bucketMetadata));
                break;
            case "B":
                Assert.assertEquals(lift, 2.91, JsonUtils.serialize(bucketMetadata));
                Assert.assertEquals(bucketMetadata.getAverageExpectedRevenue(), 111.90985217981834,
                        JsonUtils.serialize(bucketMetadata));
                Assert.assertEquals(bucketMetadata.getTotalExpectedRevenue(), 95347.19405720523,
                        JsonUtils.serialize(bucketMetadata));
                break;
            case "C":
                Assert.assertEquals(lift, 0.49, JsonUtils.serialize(bucketMetadata));
                Assert.assertEquals(bucketMetadata.getAverageExpectedRevenue(), 18.83672877172994,
                        JsonUtils.serialize(bucketMetadata));
                Assert.assertEquals(bucketMetadata.getTotalExpectedRevenue(), 44567.70027391304,
                        JsonUtils.serialize(bucketMetadata));
                break;
            case "D":
                Assert.assertEquals(lift, 0.01, JsonUtils.serialize(bucketMetadata));
                Assert.assertEquals(bucketMetadata.getAverageExpectedRevenue(), 0.20365530475782825,
                        JsonUtils.serialize(bucketMetadata));
                Assert.assertEquals(bucketMetadata.getTotalExpectedRevenue(), 1156.5584757197066,
                        JsonUtils.serialize(bucketMetadata));
                break;
            }
        }
        Integer sumCount = bucketMetadataList.stream().map(BucketMetadata::getNumLeads).reduce(0, (a, b) -> a + b);
        Assert.assertEquals(sumCount, new Integer(summary.getTotalNumLeads()));
    }

    private static List<BucketMetadata> getBucketMetadata() {
        List<BucketMetadata> buckets = new ArrayList<>();
        buckets.add(bucket(99, 94, BucketName.A));
        buckets.add(bucket(94, 85, BucketName.B));
        buckets.add(bucket(85, 60, BucketName.C));
        buckets.add(bucket(60, 5, BucketName.D));
        return buckets;
    }

    @SuppressWarnings("deprecation")
    private static BucketMetadata bucket(int leftBoundScore, int rightBoundScore, BucketName bucketName) {
        BucketMetadata bucket = new BucketMetadata();
        bucket.setLeftBoundScore(leftBoundScore);
        bucket.setRightBoundScore(rightBoundScore);
        bucket.setBucket(bucketName);
        return bucket;
    }

}
