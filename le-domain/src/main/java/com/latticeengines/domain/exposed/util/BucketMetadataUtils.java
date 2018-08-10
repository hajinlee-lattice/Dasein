package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;

public class BucketMetadataUtils {

    private static final Logger log = LoggerFactory.getLogger(BucketMetadataUtils.class);

    /**
     * @param sortedBucketMetadataList
     *            should be sorted by lower bound asc
     */
    public static BucketMetadata bucketMetadata(List<BucketMetadata> sortedBucketMetadataList, double score) {
        BucketMetadata result = null;
        for (BucketMetadata bucketMetadata : sortedBucketMetadataList) {
            if (score >= bucketMetadata.getRightBoundScore()) {
                result = bucketMetadata;
            } else {
                break;
            }
        }
        if (result == null) {
            BucketMetadata minBucket = sortedBucketMetadataList.get(0);
            log.warn(String.format("%f is less than minimum bound, setting to %s", score, minBucket.getBucketName()));
            result = minBucket;
        }
        return result;
    }

    public static List<BucketMetadata> getDefaultMetadata() {
        List<BucketMetadata> buckets = new ArrayList<>();
        buckets.add(bucket(99, 95, BucketName.A));
        buckets.add(bucket(94, 85, BucketName.B));
        buckets.add(bucket(84, 50, BucketName.C));
        buckets.add(bucket(49, 5, BucketName.D));
        return buckets;
    }

    public static BucketMetadata bucket(int leftBoundScore, int rightBoundScore, BucketName bucketName) {
        BucketMetadata bucket = new BucketMetadata();
        bucket.setLeftBoundScore(leftBoundScore);
        bucket.setRightBoundScore(rightBoundScore);
        bucket.setBucket(bucketName);
        return bucket;
    }

}
