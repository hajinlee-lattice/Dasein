package com.latticeengines.apps.cdl.workflow;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;

public class RatingEngineBucketBuilder {
    public List<BucketMetadata> build(boolean expectedValue, boolean liftChart) {
        List<BucketMetadata> buckets = new ArrayList<>();
        if (expectedValue && liftChart) {
            buckets.add(addBucket(10, 4, BucketName.A));
            buckets.add(addBucket(4, 2, BucketName.B));
            buckets.add(addBucket(2, 1, BucketName.C));
            buckets.add(addBucket(1, 0, BucketName.D));
            return buckets;
        }
        if (expectedValue && !liftChart) {
            buckets.add(addBucket(100000, 50000, BucketName.A));
            buckets.add(addBucket(50000, 10000, BucketName.B));
            buckets.add(addBucket(10000, 1000, BucketName.C));
            buckets.add(addBucket(1000, 0, BucketName.D));
            return buckets;
        }
        if (!expectedValue && liftChart) {
            buckets.add(addBucket(10, 4, BucketName.A));
            buckets.add(addBucket(4, 2, BucketName.B));
            buckets.add(addBucket(2, 1, BucketName.C));
            buckets.add(addBucket(1, 0, BucketName.D));
            return buckets;
        }
        if (!expectedValue && !liftChart) {
            buckets.add(addBucket(95, 85, BucketName.A));
            buckets.add(addBucket(85, 75, BucketName.B));
            buckets.add(addBucket(75, 50, BucketName.C));
            buckets.add(addBucket(50, 0, BucketName.D));
            return buckets;
        }
        return buckets;
    }

    @SuppressWarnings("deprecation")
    private BucketMetadata addBucket(int leftBoundScore, int rightBoundScore, BucketName bucketName) {
        BucketMetadata bucket = new BucketMetadata();
        bucket.setLeftBoundScore(leftBoundScore);
        bucket.setRightBoundScore(rightBoundScore);
        bucket.setBucket(bucketName);
        return bucket;
    }

}
