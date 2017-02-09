package com.latticeengines.leadprioritization.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.scoring.ScoreRating;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings({ "rawtypes", "serial" })
public class AddRatingColumnFunction extends BaseOperation implements Function {

    private static final Log log = LogFactory.getLog(AddRatingColumnFunction.class);

    private String scoreFieldName;
    private List<BucketMetadata> bucketMetadata = new ArrayList<>();

    public AddRatingColumnFunction(String scoreFieldName, String ratingFieldName, List<BucketMetadata> bucketMetadata) {
        super(new Fields(ratingFieldName));
        this.scoreFieldName = scoreFieldName;
        this.bucketMetadata = bucketMetadata;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Object scoreObj = arguments.getObject(scoreFieldName);
        int percentileScore = Integer.parseInt(scoreObj.toString());
        Object ratingObj = bucketPercileScore(bucketMetadata, percentileScore);
        functionCall.getOutputCollector().add(new Tuple(ratingObj.toString()));
        return;
    }

    protected BucketName bucketPercileScore(List<BucketMetadata> bucketMetadataList, int percentile) {
        BucketName bucketName = null;
        int min = ScoreRating.BUCKET_A_UPPER;
        int max = ScoreRating.BUCKET_D_LOWER;
        BucketName minBucket = null;
        BucketName maxBucket = null;
        boolean withinRange = false;
        if (bucketMetadataList != null && !bucketMetadataList.isEmpty()) {
            for (BucketMetadata bucketMetadata : bucketMetadataList) {
                // leftBoundScore is the upper bound, and the rightBoundScore is
                // the lower bound
                int leftBoundScore = bucketMetadata.getLeftBoundScore();
                int rightBoundScore = bucketMetadata.getRightBoundScore();
                BucketName currentBucketName = bucketMetadata.getBucketName();
                if (rightBoundScore < min) {
                    min = rightBoundScore;
                    minBucket = currentBucketName;
                }
                if (leftBoundScore > max) {
                    max = leftBoundScore;
                    maxBucket = currentBucketName;
                }
                if (percentile >= rightBoundScore && percentile <= leftBoundScore) {
                    withinRange = true;
                    bucketName = currentBucketName;
                }
            }
            if (log.isDebugEnabled()) {
                log.debug(String.format("min: %d, manx: %d", min, max));
            }
            if (min > max) {
                throw new RuntimeException("Bucket metadata has wrong buckets");
            }

            if (!withinRange && percentile < min) {
                log.warn(String.format("%d is less than minimum bound, setting to %s", percentile,
                        minBucket.toString()));
                bucketName = minBucket;
            } else if (!withinRange && percentile > max) {
                log.warn(String.format("%d is more than maximum bound, setting to %s", percentile,
                        maxBucket.toString()));
                bucketName = maxBucket;
            }
        } else {
            // use default bucketing criteria
            if (log.isDebugEnabled()) {
                log.debug("No bucket metadata is defined, therefore use default bucketing criteria.");
            }
            if (percentile < ScoreRating.BUCKET_D_LOWER) {
                log.warn(
                        String.format("%d is less than minimum bound, setting to %s", percentile, BucketName.D.name()));
                bucketName = BucketName.D;
            } else if (percentile >= ScoreRating.BUCKET_D_LOWER && percentile <= ScoreRating.BUCKET_D_UPPER) {
                bucketName = BucketName.D;
            } else if (percentile >= ScoreRating.BUCKET_C_LOWER && percentile <= ScoreRating.BUCKET_C_UPPER) {
                bucketName = BucketName.C;
            } else if (percentile >= ScoreRating.BUCKET_B_LOWER && percentile <= ScoreRating.BUCKET_B_UPPER) {
                bucketName = BucketName.B;
            } else if (percentile >= ScoreRating.BUCKET_A_LOWER && percentile <= ScoreRating.BUCKET_A_UPPER) {
                bucketName = BucketName.A;
            } else {
                log.warn(
                        String.format("%d is more than maximum bound, setting to %s", percentile, BucketName.A.name()));
                bucketName = BucketName.A;
            }
        }
        return bucketName;
    }

}
