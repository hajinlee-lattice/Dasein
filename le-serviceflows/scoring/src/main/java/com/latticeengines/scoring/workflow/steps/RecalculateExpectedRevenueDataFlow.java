package com.latticeengines.scoring.workflow.steps;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.RecalculateExpectedRevenueParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.RecalculateExpectedRevenueDataFlowConfiguration;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.scoring.workflow.util.ScoreArtifactRetriever;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("recalculateExpectedRevenue")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RecalculateExpectedRevenueDataFlow extends RunDataFlow<RecalculateExpectedRevenueDataFlowConfiguration> {
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

        RecalculateExpectedRevenueParameters params = new RecalculateExpectedRevenueParameters();
        params.setInputTableName(inputTableName);
        params.setPercentileFieldName(ScoreResultField.Percentile.displayName);
        params.setPredictedRevenuePercentileFieldName(ScoreResultField.PredictedRevenuePercentile.displayName);
        params.setExpectedRevenueFieldName(ScoreResultField.ExpectedRevenue.displayName);
        params.setModelGuidField(ScoreResultField.ModelId.displayName);

        Map<String, String> scoreFieldMap = getScoreFieldsMap();
        if (MapUtils.isNotEmpty(scoreFieldMap)) {
            params.setOriginalScoreFieldMap(scoreFieldMap);
            params.setFitFunctionParametersMap(getEVFitFunctionParametersMap(getEVModelIds(scoreFieldMap)));
        }
        configuration.setDataFlowParams(params);
    }

    private Map<String, String> getEVFitFunctionParametersMap(Collection<String> modelIds) {
        String internalResourceHostPort = configuration.getInternalResourceHostPort();
        InternalResourceRestApiProxy internalResourceRestApiProxy = new InternalResourceRestApiProxy(
            internalResourceHostPort);
        ScoreArtifactRetriever scoreArtifactRetriever = new ScoreArtifactRetriever(internalResourceRestApiProxy,
                                                                                   yarnConfiguration);
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        Map<String, String> fitFunctionParametersMap = new HashMap<>();
        for (String modelId : modelIds) {
            String fitFunctionParameters = scoreArtifactRetriever.getEVFitFunctionParameters(customerSpace, modelId);
            if (fitFunctionParameters != null) {
                fitFunctionParametersMap.put(modelId, fitFunctionParameters);
            }
        }
        return fitFunctionParametersMap;
    }

    private Collection<String> getEVModelIds(Map<String, String> modelFieldMap) {
        return modelFieldMap.entrySet().stream()
            .filter(entry -> ScoreResultField.ExpectedRevenue.displayName.equals(entry.getValue()))
            .map(Map.Entry::getKey).collect(Collectors.toList());
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
                scoreField = ScoreResultField.ExpectedRevenue.displayName;
            }
            originalScoreFieldsMap.put(modelGuid, scoreField);
        });
        return originalScoreFieldsMap;
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
