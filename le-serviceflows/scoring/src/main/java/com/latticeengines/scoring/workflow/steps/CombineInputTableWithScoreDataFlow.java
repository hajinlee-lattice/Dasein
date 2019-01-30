package com.latticeengines.scoring.workflow.steps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.CombineInputTableWithScoreDataFlowConfiguration;
import com.latticeengines.domain.exposed.util.BucketMetadataUtils;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("combineInputTableWithScoreDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
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
        putStringValueInContext(EXPORT_SCORE_TRAINING_FILE_TABLE_NAME, configuration.getTargetTableName());
        // putStringValueInContext(COMPUTE_LIFT_INPUT_TABLE_NAME,
        // configuration.getTargetTableName());
        putStringValueInContext(PIVOT_SCORE_INPUT_TABLE_NAME, configuration.getTargetTableName());
        putStringValueInContext(AI_RAW_RATING_TABLE_NAME, configuration.getTargetTableName());
    }

    private void setupDataFlow() {
        CombineInputTableWithScoreParameters params = new CombineInputTableWithScoreParameters(
                getScoreResultTableName(), getInputTableName(), getBucketMetadata(), getModelType(),
                configuration.getIdColumnName());
        if (configuration.isCdlMultiModel()) {
            setCdlMultiModelParams(params);
        }
        configuration.setDataFlowParams(params);
        configuration.setJobProperties(initJobProperties());
    }

    private void setCdlMultiModelParams(CombineInputTableWithScoreParameters params) {
        if (!configuration.isCdlMultiModel())
            return;

        List<RatingModelContainer> containers = getModelContainers();
        if (CollectionUtils.isEmpty(containers)) {
            throw new IllegalStateException("There is no AI models in context.");
        }
        Map<String, List<BucketMetadata>> bucketMetadataMap = new HashMap<>();

        containers.forEach(container -> {
            CombineInputTableWithScoreParameters singleModelParams = getSingleModelParams(container);
            String modelGuid = ((AIModel) container.getModel()).getModelSummaryId();
            bucketMetadataMap.put(modelGuid, singleModelParams.getBucketMetadata());
        });

        params.setBucketMetadataMap(bucketMetadataMap);
        params.setScoreFieldName(InterfaceName.Score.name());
        params.setIdColumn(InterfaceName.__Composite_Key__.toString());
        params.setModelIdField(ScoreResultField.ModelId.displayName);
    }

    private CombineInputTableWithScoreParameters getSingleModelParams(RatingModelContainer container) {
        CombineInputTableWithScoreParameters params = new CombineInputTableWithScoreParameters(
                getScoreResultTableName(), getInputTableName());
        AIModel aiModel = (AIModel) container.getModel();
        List<BucketMetadata> bucketMetadata = container.getScoringBucketMetadata();
        if (CollectionUtils.isEmpty(bucketMetadata)) {
            throw new IllegalArgumentException("AI model " + aiModel.getId() + " does not have bucket metadata.");
        }
        params.setBucketMetadata(bucketMetadata);
        return params;
    }

    private List<RatingModelContainer> getModelContainers() {
        List<RatingModelContainer> allContainers = getListObjectFromContext(ITERATION_RATING_MODELS,
                RatingModelContainer.class);
        return allContainers.stream() //
                .filter(container -> {
                    RatingEngineType ratingEngineType = container.getEngineSummary().getType();
                    return RatingEngineType.CROSS_SELL.equals(ratingEngineType)
                            || RatingEngineType.CUSTOM_EVENT.equals(ratingEngineType);
                }) //
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
        List<BucketMetadata> bucketMetadata = configuration.getBucketMetadata();
        if (CollectionUtils.isEmpty(bucketMetadata)) {
            bucketMetadata = BucketMetadataUtils.getDefaultMetadata();
        }
        putObjectInContext(SCORING_BUCKET_METADATA, bucketMetadata);
        return bucketMetadata;
    }

    private String getModelType() {
        return configuration.getModelType();
    }
}
