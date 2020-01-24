package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;

public final class BucketMetadataUtils {

    protected BucketMetadataUtils() {
        throw new UnsupportedOperationException();
    }

    private static final Logger log = LoggerFactory.getLogger(BucketMetadataUtils.class);

    /**
     * @param sortedBucketMetadataList
     *            should be sorted by lower bound asc
     */
    public static BucketMetadata bucketMetadata(List<BucketMetadata> sortedBucketMetadataList,
            double score) {
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
            log.warn(String.format("%f is less than minimum bound, setting to %s", score,
                    minBucket.getBucketName()));
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

    @SuppressWarnings("deprecation")
    public static BucketMetadata bucket(int leftBoundScore, int rightBoundScore,
            BucketName bucketName) {
        BucketMetadata bucket = new BucketMetadata();
        bucket.setLeftBoundScore(leftBoundScore);
        bucket.setRightBoundScore(rightBoundScore);
        bucket.setBucket(bucketName);
        return bucket;
    }

    public static String bucketScore(List<BucketMetadata> bucketMetadata, double score) {
        BucketName bucketName;
        if (CollectionUtils.isNotEmpty(bucketMetadata)) {
            bucketName = BucketMetadataUtils.bucketMetadata(bucketMetadata, score).getBucket();
        } else {
            if (log.isDebugEnabled()) {
                log.debug("No bucket metadata is defined, therefore use default bucketing criteria.");
            }
            if (score < BucketName.C.getDefaultLowerBound()) {
                if (score < BucketName.D.getDefaultLowerBound()) {
                    log.warn(String.format("%f is less than minimum bound, setting to %s", score, BucketName.D.name()));
                }
                bucketName = BucketName.D;
            } else if (score < BucketName.B.getDefaultLowerBound()) {
                bucketName = BucketName.C;
            } else if (score < BucketName.A.getDefaultLowerBound()) {
                bucketName = BucketName.B;
            } else {
                if (score > BucketName.A.getDefaultUpperBound()) {
                    log.warn(String.format("%f is more than maximum bound, setting to %s", score, BucketName.A.name()));
                }
                bucketName = BucketName.A;
            }
        }
        return bucketName == null ? null : bucketName.toValue();
    }

}
