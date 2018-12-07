package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;

public class BucketMetadataUtilsUnitTestNG {

    private List<BucketMetadata> buckets;

    @BeforeClass(groups = "unit")
    public void setup() {
        buckets = getBucketMetadataList();
    }

    @Test(groups = "unit", dataProvider = "dataProvider")
    public void test(double score, BucketName expectedBucket) {
        Assert.assertEquals(BucketMetadataUtils.bucketMetadata(buckets, score).getBucket(),
                expectedBucket);
    }

    @DataProvider(name = "dataProvider")
    public Object[][] provideData() {
        return new Object[][] { //
                { 0.5, BucketName.D }, //
                { 1.0, BucketName.C }, //
                { 5.0, BucketName.A }, //
                { 15.0, BucketName.A }, //
        };
    }

    private List<BucketMetadata> getBucketMetadataList() {
        List<BucketMetadata> buckets = new ArrayList<>();
        buckets.add(addBucket(10, 4, BucketName.A));
        buckets.add(addBucket(4, 2, BucketName.B));
        buckets.add(addBucket(2, 1, BucketName.C));
        buckets.add(addBucket(1, 0, BucketName.D));
        return buckets.stream() //
                .sorted(Comparator.comparingInt(BucketMetadata::getRightBoundScore)) //
                .collect(Collectors.toList());
    }

    @SuppressWarnings("deprecation")
    private BucketMetadata addBucket(int leftBoundScore, int rightBoundScore,
            BucketName bucketName) {
        BucketMetadata bucket = new BucketMetadata();
        bucket.setLeftBoundScore(leftBoundScore);
        bucket.setRightBoundScore(rightBoundScore);
        bucket.setBucket(bucketName);
        return bucket;
    }

}
