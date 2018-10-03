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
        BucketedScoreSummary summary = BucketedScoreSummaryUtils.generateBucketedScoreSummary(recordList);
        Assert.assertEquals(summary.getBarLifts().length, 32);
        List<BucketedScore> notNullBuckets = Arrays.stream(summary.getBucketedScores()).filter(Objects::nonNull).collect(Collectors.toList());
        Assert.assertEquals(notNullBuckets.size(), 96);
        Assert.assertEquals(notNullBuckets.stream().map(BucketedScore::getNumLeads).reduce(0, (a, b) -> a + b), new Integer(summary.getTotalNumLeads()));
    }

    @Test(groups = "unit")
    public void testComputeLift() throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream is = classLoader.getResourceAsStream(RESOURCE_ROOT + "bucketed_score_summary.json");
        ObjectMapper om = new ObjectMapper();
        BucketedScoreSummary summary = om.readValue(is, BucketedScoreSummary.class);
        List<BucketMetadata> bucketMetadataList = getBucketMetadata();
        bucketMetadataList = BucketedScoreSummaryUtils.computeLift(summary, bucketMetadataList);
        for (BucketMetadata bucketMetadata: bucketMetadataList) {
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

    private static List<BucketMetadata> getBucketMetadata() {
        List<BucketMetadata> buckets = new ArrayList<>();
        buckets.add(bucket(99, 94, BucketName.A));
        buckets.add(bucket(94, 85, BucketName.B));
        buckets.add(bucket(85, 60, BucketName.C));
        buckets.add(bucket(60, 5, BucketName.D));
        return buckets;
    }

    private static BucketMetadata bucket(int leftBoundScore, int rightBoundScore, BucketName bucketName) {
        BucketMetadata bucket = new BucketMetadata();
        bucket.setLeftBoundScore(leftBoundScore);
        bucket.setRightBoundScore(rightBoundScore);
        bucket.setBucket(bucketName);
        return bucket;
    }


}
