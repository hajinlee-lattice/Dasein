package com.latticeengines.domain.exposed.util;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.pls.BucketMetadata;

public class BucketMetadataUtils {

    private static final Logger log = LoggerFactory.getLogger(BucketMetadataUtils.class);

    /**
     * @param sortedBucketMetadataList should be sorted by lower bound asc
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

}
