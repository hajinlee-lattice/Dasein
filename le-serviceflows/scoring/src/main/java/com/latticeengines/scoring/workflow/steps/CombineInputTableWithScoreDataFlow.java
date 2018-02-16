package com.latticeengines.scoring.workflow.steps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
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
        configureExport();
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
        if (configuration.isExpectedValue())
            scoreFieldName = InterfaceName.ExpectedRevenue.name();
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

        containers.forEach(container -> {
            CombineInputTableWithScoreParameters singleModelParams = getSingleModelParams(container);
            String modelGuid = ((AIModel) container.getModel()).getModelSummary().getId();
            bucketMetadataMap.put(modelGuid, singleModelParams.getBucketMetadata());
            scoreFieldMap.put(modelGuid, singleModelParams.getScoreFieldName());
            scoreMultiplierMap.put(modelGuid, singleModelParams.getScoreMultiplier());
        });

        params.setBucketMetadataMap(bucketMetadataMap);
        params.setScoreFieldMap(scoreFieldMap);
        params.setScoreMultiplierMap(scoreMultiplierMap);

        if (configuration.isLiftChart())
            params.setAvgScore(getDoubleValueFromContext(SCORING_AVG_SCORE));

        params.setIdColumn(InterfaceName.__Composite_Key__.toString());
    }

    private CombineInputTableWithScoreParameters getSingleModelParams(RatingModelContainer container) {
        CombineInputTableWithScoreParameters params =
                new CombineInputTableWithScoreParameters(getScoreResultTableName(), getInputTableName());
        return params;
    }

    private List<RatingModelContainer> getModelContainers() {
        List<RatingModelContainer> allContainers = getListObjectFromContext(RATING_MODELS, RatingModelContainer.class);
        return allContainers.stream() //
                .filter(container -> RatingEngineType.AI_BASED.equals(container.getEngineSummary().getType())) //
                .collect(Collectors.toList());
    }

    private void configureExport() {
        putStringValueInContext(EXPORT_TABLE_NAME, configuration.getTargetTableName());
    }

    private String getInputTableName() {
        return getDataFlowParams().getInputTableName();
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
