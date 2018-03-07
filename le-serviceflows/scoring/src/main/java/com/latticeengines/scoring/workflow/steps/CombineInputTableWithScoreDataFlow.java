package com.latticeengines.scoring.workflow.steps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.CombineInputTableWithScoreDataFlowConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("combineInputTableWithScoreDataFlow")
public class CombineInputTableWithScoreDataFlow extends RunDataFlow<CombineInputTableWithScoreDataFlowConfiguration> {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(CombineInputTableWithScoreDataFlow.class);

    @Override
    public void execute() {
        setupDataFlow();
        super.execute();
    }

    @Override
    public void onExecutionCompleted() {
        putStringValueInContext(EXPORT_TABLE_NAME, configuration.getTargetTableName());
        putStringValueInContext(AI_RAW_RATING_TABLE_NAME, configuration.getTargetTableName());
    }

    private void setupDataFlow() {
        CombineInputTableWithScoreParameters params = new CombineInputTableWithScoreParameters(
                getScoreResultTableName(), getInputTableName(), getBucketMetadata(), getModelType());
        if (configuration.isCdlModel()) {
            setupCdlParameters(params);
        } else if (configuration.isCdlMultiModel()) {
            setCdlMultiModelParams(params);
        }
        configuration.setDataFlowParams(params);
    }

    private void setupCdlParameters(CombineInputTableWithScoreParameters params) {
        if (!configuration.isCdlModel())
            return;
        String scoreFieldName = InterfaceName.Probability.name();
        if (configuration.isExpectedValue()) {
            scoreFieldName = InterfaceName.ExpectedRevenue.name();
        }
        params.setScoreFieldName(scoreFieldName);
        Integer multiplier = null;
        if (!configuration.isExpectedValue() && !configuration.isLiftChart())
            multiplier = 100;
        params.setScoreMultiplier(multiplier);
        if (configuration.isLiftChart())
            params.setAvgScore(getDoubleValueFromContext(SCORING_AVG_SCORE));
        params.setIdColumn(InterfaceName.AnalyticPurchaseState_ID.toString());
    }

    private void setCdlMultiModelParams(CombineInputTableWithScoreParameters params) {
        if (!configuration.isCdlMultiModel())
            return;

        List<RatingModelContainer> containers = getModelContainers();
        if (CollectionUtils.isEmpty(containers)) {
            throw new IllegalStateException("There is no AI models in context.");
        }
        Map<String, List<BucketMetadata>> bucketMetadataMap = new HashMap<>();
        Map<String, String> scoreFieldMap = new HashMap<>();
        Map<String, Integer> scoreMultiplierMap = new HashMap<>();

        Map<String, PredictionType> predictionTypeMap = new HashMap<>();
        containers.forEach(container -> {
            CombineInputTableWithScoreParameters singleModelParams = getSingleModelParams(container);
            String modelGuid = ((AIModel) container.getModel()).getModelSummaryId();
            predictionTypeMap.put(modelGuid, singleModelParams.getPredictionType());
            bucketMetadataMap.put(modelGuid, singleModelParams.getBucketMetadata());
            scoreFieldMap.put(modelGuid, singleModelParams.getScoreFieldName());
            scoreMultiplierMap.put(modelGuid, singleModelParams.getScoreMultiplier());
        });

        params.setBucketMetadataMap(bucketMetadataMap);
        params.setScoreFieldMap(scoreFieldMap);
        params.setScoreMultiplierMap(scoreMultiplierMap);
        params.setScoreAvgMap(getMapObjectFromContext(SCORING_AVG_SCORES, String.class, Double.class));
        params.setIdColumn(InterfaceName.__Composite_Key__.toString());
        params.setModelIdField("Model_GUID");
        putObjectInContext(SCORING_SCORE_FIELDS, scoreFieldMap);
        putObjectInContext(PREDICTION_TYPES, predictionTypeMap);
    }

    private CombineInputTableWithScoreParameters getSingleModelParams(RatingModelContainer container) {
        CombineInputTableWithScoreParameters params = new CombineInputTableWithScoreParameters(
                getScoreResultTableName(), getInputTableName());
        AIModel aiModel = (AIModel) container.getModel();
        PredictionType predictionType = aiModel.getPredictionType();
        params.setBucketMetadata(getDefaultBucketMetadata(predictionType));
        params.setScoreFieldName(getScoreFieldName(predictionType));
        params.setPredictionType(predictionType);
        // no multiplier, because always calculate lift chart
        params.setScoreMultiplier(null);
        return params;
    }

    private String getScoreFieldName(PredictionType predictionType) {
        String scoreField;
        switch (predictionType) {
        case PROPENSITY:
            scoreField = InterfaceName.Probability.name();
            break;
        case EXPECTED_VALUE:
            scoreField = InterfaceName.ExpectedRevenue.name();
            break;
        default:
            throw new UnsupportedOperationException("Unknown prediction type: " + predictionType);
        }
        return scoreField;
    }

    private List<BucketMetadata> getDefaultBucketMetadata(PredictionType predictionType) {
        List<BucketMetadata> buckets = new ArrayList<>();
        switch (predictionType) {
        case PROPENSITY:
            buckets.add(addBucket(10, 4, BucketName.A));
            buckets.add(addBucket(4, 2, BucketName.B));
            buckets.add(addBucket(2, 1, BucketName.C));
            buckets.add(addBucket(1, 0, BucketName.D));
            break;
        case EXPECTED_VALUE:
            buckets.add(addBucket(10, 4, BucketName.A));
            buckets.add(addBucket(4, 2, BucketName.B));
            buckets.add(addBucket(2, 1, BucketName.C));
            buckets.add(addBucket(1, 0, BucketName.D));
            break;
        default:
            throw new UnsupportedOperationException("Unknown prediction type: " + predictionType);
        }
        return buckets;
    }

    private BucketMetadata addBucket(int leftBoundScore, int rightBoundScore, BucketName bucketName) {
        BucketMetadata bucket = new BucketMetadata();
        bucket.setLeftBoundScore(leftBoundScore);
        bucket.setRightBoundScore(rightBoundScore);
        bucket.setBucket(bucketName);
        return bucket;
    }

    private List<RatingModelContainer> getModelContainers() {
        List<RatingModelContainer> allContainers = getListObjectFromContext(RATING_MODELS, RatingModelContainer.class);
        return allContainers.stream() //
                .filter(container -> RatingEngineType.CROSS_SELL.equals(container.getEngineSummary().getType())) //
                .collect(Collectors.toList());
    }

    private String getInputTableName() {
        String inputTableName = getStringValueFromContext(FILTER_EVENT_TARGET_TABLE_NAME);
        if (StringUtils.isBlank(inputTableName)) {
            inputTableName = getDataFlowParams().getInputTableName();
        }
        return inputTableName;
    }

    private String getScoreResultTableName() {
        String scoreResultTableName = getStringValueFromContext(SCORING_RESULT_TABLE_NAME);
        if (scoreResultTableName == null) {
            scoreResultTableName = getDataFlowParams().getScoreResultsTableName();
        }
        return scoreResultTableName;
    }

    private CombineInputTableWithScoreParameters getDataFlowParams() {
        return (CombineInputTableWithScoreParameters) configuration.getDataFlowParams();
    }

    private List<BucketMetadata> getBucketMetadata() {
        return configuration.getBucketMetadata();
    }

    private String getModelType() {
        return configuration.getModelType();
    }
}
