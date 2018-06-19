package com.latticeengines.domain.exposed.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import com.latticeengines.common.exposed.util.JsonUtils;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;

public class BucketedScoreSummaryUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testParseBucketedScore() throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream is = classLoader.getResourceAsStream("com/latticeengines/domain/exposed/util/BucketedScoreSummaryUtilsUnitTestNG/part-00000.avro");
        List<GenericRecord> recordList = AvroUtils.readFromInputStream(is);
        BucketedScoreSummary summary = BucketedScoreSummaryUtils.generateBucketedScoreSummary(recordList);
        Assert.assertEquals(summary.getBarLifts().length, 32);
        long notNullBuckets = Arrays.stream(summary.getBucketedScores()).filter(Objects::nonNull).count();
        Assert.assertEquals(notNullBuckets, 96);
    }

    @Test(groups = "unit")
    public void testComputeLift() throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream is = classLoader.getResourceAsStream("com/latticeengines/domain/exposed/util/BucketedScoreSummaryUtilsUnitTestNG/bucketed_score_summary.json");
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
                    Assert.assertEquals(lift, 3.27, JsonUtils.serialize(bucketMetadata));
                    break;
                case "C":
                    Assert.assertEquals(lift, 1.49, JsonUtils.serialize(bucketMetadata));
                    break;
                case "D":
                    Assert.assertEquals(lift, 0.04, JsonUtils.serialize(bucketMetadata));
                    break;
            }
        }
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
