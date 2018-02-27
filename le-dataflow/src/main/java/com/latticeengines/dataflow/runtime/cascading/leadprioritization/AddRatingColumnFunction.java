package com.latticeengines.dataflow.runtime.cascading.leadprioritization;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.util.BucketMetadataUtils;

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
    private String modelIdFieldName;
    private Integer scoreMultiplier;
    private Double avgScore;

    // fields for multi model
    private boolean multiModelMode = false;
    private List<BucketMetadata> bucketMetadata;
    private Map<String, List<BucketMetadata>> bucketMetadataMap;
    private Map<String, String> scoreFieldMap;
    private Map<String, Integer> scoreMultiplierMap;
    private Map<String, Double> scoreAvgMap;

    public AddRatingColumnFunction(String scoreFieldName, String ratingFieldName, List<BucketMetadata> bucketMetadata,
            Integer scoreMultiplier, Double avgScore) {
        super(new Fields(ratingFieldName));
        this.scoreFieldName = scoreFieldName;
        this.bucketMetadata = sortBucketMetadata(bucketMetadata);
        this.scoreMultiplier = scoreMultiplier;
        this.avgScore = avgScore;
    }

    public AddRatingColumnFunction(Map<String, String> scoreFieldMap, String modelIdFieldName, String ratingFieldName,
            Map<String, List<BucketMetadata>> bucketMetadataMap, Map<String, Integer> scoreMultiplierMap, Map<String, Double> scoreAvgMap) {
        super(new Fields(ratingFieldName));
        this.multiModelMode = true;
        this.scoreFieldMap = scoreFieldMap;
        this.modelIdFieldName = modelIdFieldName;
        this.scoreMultiplierMap = scoreMultiplierMap;
        this.scoreAvgMap = scoreAvgMap;
        this.bucketMetadataMap = new HashMap<>();
        bucketMetadataMap.forEach((modelId, bucketMetadata) -> //
                this.bucketMetadataMap.put(modelId, sortBucketMetadata(bucketMetadata)));
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String scoreField = scoreFieldName;
        if (multiModelMode) {
            String modelId = arguments.getString(modelIdFieldName);
            scoreField = scoreFieldMap.get(modelId);
        }
        Object scoreObj = arguments.getObject(scoreField);
        double score = Double.parseDouble(scoreObj.toString());
        if (multiModelMode) {
            String modelId = arguments.getString(modelIdFieldName);
            Double newAvgScore = scoreAvgMap.get(modelId);
            Integer scoreMultiplier = scoreMultiplierMap.get(modelId);
            if (scoreMultiplier != null) {
                score *= scoreMultiplier;
                if (newAvgScore != null) {
                    newAvgScore *= scoreMultiplier;
                }
            }
            if (newAvgScore != null) {
                score /= newAvgScore;
            }
        } else {
            Double newAvgScore = avgScore;
            if (scoreMultiplier != null) {
                score *= scoreMultiplier;
                if (newAvgScore != null) {
                    newAvgScore *= scoreMultiplier;
                }
            }
            if (newAvgScore != null) {
                score /= newAvgScore;
            }
        }
        Object ratingObj;
        if (multiModelMode) {
            String modelId = arguments.getString(modelIdFieldName);
            ratingObj = bucketScore(bucketMetadataMap.get(modelId), score);
        } else {
            ratingObj = bucketScore(bucketMetadata, score);
        }
        functionCall.getOutputCollector().add(new Tuple(ratingObj));
    }

    private String bucketScore(List<BucketMetadata> bucketMetadata, double score) {
        BucketName bucketName;
        if (CollectionUtils.isNotEmpty(bucketMetadata)) {
            bucketName = BucketMetadataUtils.bucketMetadata(bucketMetadata, score).getBucket();
        } else {
            // use default bucketing criteria
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

    private List<BucketMetadata> sortBucketMetadata(List<BucketMetadata> bucketMetadata) {
        return bucketMetadata.stream().sorted(Comparator.comparingInt(BucketMetadata::getRightBoundScore)).collect(Collectors.toList());
    }

}
