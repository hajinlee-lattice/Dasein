package com.latticeengines.dataflow.runtime.cascading.leadprioritization;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings({ "rawtypes", "serial" })
public class AddRatingColumnFunction extends BaseOperation implements Function {

    private static final Logger log = LoggerFactory.getLogger(AddRatingColumnFunction.class);

    private String scoreFieldName;
    private List<BucketMetadata> bucketMetadata = new ArrayList<>();

    private Integer scoreMultiplier;

    public AddRatingColumnFunction(String scoreFieldName, String ratingFieldName, List<BucketMetadata> bucketMetadata,
            Integer scoreMultiplier) {
        super(new Fields(ratingFieldName));
        this.scoreFieldName = scoreFieldName;
        this.bucketMetadata = bucketMetadata;
        this.scoreMultiplier = scoreMultiplier;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Object scoreObj = arguments.getObject(scoreFieldName);
        double score = Double.parseDouble(scoreObj.toString());
        if (scoreMultiplier != null) {
            score *= scoreMultiplier;
        }
        Object ratingObj = bucketPercileScore(bucketMetadata, score);
        functionCall.getOutputCollector().add(new Tuple(ratingObj.toString()));
        return;
    }

    protected String bucketPercileScore(List<BucketMetadata> bucketMetadataList, double score) {
        BucketName bucketName = null;
        int min = BucketName.A.getDefaultUpperBound();
        int max = BucketName.D.getDefaultLowerBound();
        BucketName minBucket = null;
        BucketName maxBucket = null;
        boolean withinRange = false;
        if (bucketMetadataList != null && !bucketMetadataList.isEmpty()) {
            for (BucketMetadata bucketMetadata : bucketMetadataList) {
                // leftBoundScore is the upper bound, and the rightBoundScore is
                // the lower bound
                int leftBoundScore = bucketMetadata.getLeftBoundScore();
                int rightBoundScore = bucketMetadata.getRightBoundScore();
                BucketName currentBucketName = bucketMetadata.getBucket();
                if (rightBoundScore < min) {
                    min = rightBoundScore;
                    minBucket = currentBucketName;
                }
                if (leftBoundScore > max) {
                    max = leftBoundScore;
                    maxBucket = currentBucketName;
                }
                if (score >= rightBoundScore && score <= leftBoundScore) {
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

            if (!withinRange && score < min) {
                log.warn(String.format("%d is less than minimum bound, setting to %s", score, minBucket.toString()));
                bucketName = minBucket;
            } else if (!withinRange && score > max) {
                log.warn(String.format("%d is more than maximum bound, setting to %s", score, maxBucket.toString()));
                bucketName = maxBucket;
            }
        } else {
            // use default bucketing criteria
            if (log.isDebugEnabled()) {
                log.debug("No bucket metadata is defined, therefore use default bucketing criteria.");
            }
            if (score < BucketName.D.getDefaultLowerBound()) {
                log.warn(String.format("%d is less than minimum bound, setting to %s", score, BucketName.D.name()));
                bucketName = BucketName.D;
            } else if (score >= BucketName.D.getDefaultLowerBound() && score <= BucketName.D.getDefaultUpperBound()) {
                bucketName = BucketName.D;
            } else if (score >= BucketName.C.getDefaultLowerBound() && score <= BucketName.C.getDefaultUpperBound()) {
                bucketName = BucketName.C;
            } else if (score >= BucketName.B.getDefaultLowerBound() && score <= BucketName.B.getDefaultUpperBound()) {
                bucketName = BucketName.B;
            } else if (score >= BucketName.A.getDefaultLowerBound() && score <= BucketName.A.getDefaultUpperBound()) {
                bucketName = BucketName.A;
            } else {
                log.warn(String.format("%d is more than maximum bound, setting to %s", score, BucketName.A.name()));
                bucketName = BucketName.A;
            }
        }
        return bucketName == null ? null : bucketName.toValue();
    }

}
