package com.latticeengines.dataflow.runtime.cascading.leadprioritization;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

    protected String bucketPercileScore(List<BucketMetadata> bucketMetadataList, int percentile) {
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
            if (percentile < BucketName.D.getDefaultLowerBound()) {
                log.warn(
                        String.format("%d is less than minimum bound, setting to %s", percentile, BucketName.D.name()));
                bucketName = BucketName.D;
            } else if (percentile >= BucketName.D.getDefaultLowerBound()
                    && percentile <= BucketName.D.getDefaultUpperBound()) {
                bucketName = BucketName.D;
            } else if (percentile >= BucketName.C.getDefaultLowerBound()
                    && percentile <= BucketName.C.getDefaultUpperBound()) {
                bucketName = BucketName.C;
            } else if (percentile >= BucketName.B.getDefaultLowerBound()
                    && percentile <= BucketName.B.getDefaultUpperBound()) {
                bucketName = BucketName.B;
            } else if (percentile >= BucketName.A.getDefaultLowerBound()
                    && percentile <= BucketName.A.getDefaultUpperBound()) {
                bucketName = BucketName.A;
            } else {
                log.warn(
                        String.format("%d is more than maximum bound, setting to %s", percentile, BucketName.A.name()));
                bucketName = BucketName.A;
            }
        }
        return bucketName == null ? null : bucketName.toValue();
    }

}
