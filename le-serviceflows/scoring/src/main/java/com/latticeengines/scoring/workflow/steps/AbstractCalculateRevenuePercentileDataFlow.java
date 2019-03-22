package com.latticeengines.scoring.workflow.steps;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DataFlowStepConfiguration;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

public abstract class AbstractCalculateRevenuePercentileDataFlow<T extends DataFlowStepConfiguration>
        extends RunDataFlow<T> {
    private static final Logger log = LoggerFactory.getLogger(AbstractCalculateRevenuePercentileDataFlow.class);

    private static final String modelGuidField = ScoreResultField.ModelId.displayName;

    private static final int percentileLowerBound = 5;

    private static final int percentileUpperBound = 99;

    @Inject
    public ModelSummaryProxy modelSummaryProxy;

    abstract String getRevenueFieldName();

    abstract boolean shouldLoadNormalizationRatio();

    abstract void initAndSetDataFlowParam(String inputTableName, String modelGuidField, int percentileLowerBound,
            int percentileUpperBound, Map<String, String> originalScoreFieldMap,
            Map<String, Double> normalizationRatioMap);

    @Override
    public void execute() {
        preDataFlow();
        super.execute();
    }

    @Override
    public void onExecutionCompleted() {
        putStringValueInContext(SCORING_RESULT_TABLE_NAME, configuration.getTargetTableName());
    }

    private void preDataFlow() {
        String inputTableName = getStringValueFromContext(SCORING_RESULT_TABLE_NAME);

        Map<String, String> scoreFieldMap = getScoreFieldsMap();
        Map<String, Double> normalizationRatioMap = shouldLoadNormalizationRatio() ? getNormalizationRatioMap() : null;

        initAndSetDataFlowParam(inputTableName, modelGuidField, percentileLowerBound, percentileUpperBound,
                scoreFieldMap, normalizationRatioMap);
    }

    private Map<String, String> getScoreFieldsMap() {
        Map<String, String> originalScoreFieldsMap;
        originalScoreFieldsMap = new HashMap<>();
        List<RatingModelContainer> containers = getModelContainers();
        containers.forEach(container -> {
            AIModel aiModel = (AIModel) container.getModel();
            String modelGuid = aiModel.getModelSummaryId();
            String scoreField = InterfaceName.RawScore.name();
            if (PredictionType.EXPECTED_VALUE.equals(aiModel.getPredictionType())) {
                scoreField = getRevenueFieldName();
            }
            originalScoreFieldsMap.put(modelGuid, scoreField);
        });
        if (MapUtils.isEmpty(originalScoreFieldsMap)) {
            String modelGuid = getStringValueFromContext(SCORING_MODEL_ID);
            String modelType = getStringValueFromContext(SCORING_MODEL_TYPE);
            log.info(String.format("modelGuid = %s, modelType = %s", modelGuid, modelType));
            originalScoreFieldsMap.put(modelGuid, getRevenueFieldName());
        }
        return originalScoreFieldsMap;
    }

    private Map<String, Double> getNormalizationRatioMap() {
        Map<String, Double> normalizationRatioMap = new HashMap<>();
        List<RatingModelContainer> containers = getModelContainers();
        String tenantId = configuration.getCustomerSpace().toString();
        if (CollectionUtils.isNotEmpty(containers)) {
            containers.forEach(container -> {
                AIModel aiModel = (AIModel) container.getModel();
                String modelGuid = aiModel.getModelSummaryId();
                if (PredictionType.EXPECTED_VALUE.equals(aiModel.getPredictionType())) {
                    ModelSummary modelSummary = modelSummaryProxy.getModelSummary(tenantId, modelGuid);
                    if (modelSummary.getNormalizationRatio() != null) {
                        normalizationRatioMap.put(modelGuid, modelSummary.getNormalizationRatio());
                    }
                }
            });
        } else {
            String modelGuid = getStringValueFromContext(SCORING_MODEL_ID);
            log.info(String.format("modelGuid = %s", modelGuid));
            ModelSummary modelSummary = modelSummaryProxy.getModelSummary(tenantId, modelGuid);
            if (modelSummary.getNormalizationRatio() != null) {
                normalizationRatioMap.put(modelGuid, modelSummary.getNormalizationRatio());
            }
        }
        return normalizationRatioMap;
    }

    private List<RatingModelContainer> getModelContainers() {
        List<RatingModelContainer> allContainers = getListObjectFromContext(RATING_MODELS, RatingModelContainer.class);
        if (allContainers == null) {
            return Collections.emptyList();
        }
        return allContainers.stream() //
                .filter(container -> {
                    RatingEngineType ratingEngineType = container.getEngineSummary().getType();
                    return RatingEngineType.CROSS_SELL.equals(ratingEngineType)
                            || RatingEngineType.CUSTOM_EVENT.equals(ratingEngineType);
                }).collect(Collectors.toList());
    }
}
