package com.latticeengines.domain.exposed.util;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.pls.BucketMetadata;

public class BucketMetadataUtils {

    private static final Logger log = LoggerFactory.getLogger(BucketMetadataUtils.class);

    public static BucketMetadata bucketMetadata(List<BucketMetadata> bucketMetadataList, double score) {
        BucketMetadata result = null;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        BucketMetadata minBucket = null;
        BucketMetadata maxBucket = null;
        boolean withinRange = false;
        for (BucketMetadata bucketMetadata : bucketMetadataList) {
            // leftBoundScore is the upper bound, and the rightBoundScore is
            // the lower bound
            int leftBoundScore = bucketMetadata.getLeftBoundScore();
            int rightBoundScore = bucketMetadata.getRightBoundScore();
            BucketMetadata currentBucket = bucketMetadata;
            if (rightBoundScore < min) {
                min = rightBoundScore;
                minBucket = currentBucket;
            }
            if (leftBoundScore > max) {
                max = leftBoundScore;
                maxBucket = currentBucket;
            }
            if (score >= rightBoundScore && score <= leftBoundScore) {
                withinRange = true;
                result = currentBucket;
                break;
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("min: %d, manx: %d", min, max));
        }
        if (min > max) {
            throw new RuntimeException("Bucket metadata has wrong buckets");
        }

        if (!withinRange && score < min) {
            log.warn(String.format("%f is less than minimum bound, setting to %s", score,
                    minBucket.getBucketName().toString()));
            result = minBucket;
        } else if (!withinRange && score > max) {
            log.warn(String.format("%f is more than maximum bound, setting to %s", score,
                    maxBucket.getBucket().toString()));
            result = maxBucket;
        }
        return result;
    }

}
