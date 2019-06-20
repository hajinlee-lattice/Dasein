package com.latticeengines.scoring.workflow.steps;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.scoringapi.EVScoreDerivation;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculateExpectedRevenuePercentileParameters.ScoreDerivationType;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.scoring.workflow.util.ScoreArtifactRetriever;

public class ExpectedRevenueDataFlowUtil {
    private static final Logger log = LoggerFactory.getLogger(ExpectedRevenueDataFlowUtil.class);

    public static Map<String, String> getEVFitFunctionParametersMap(CustomerSpace customerSpace,
            Configuration yarnConfiguration, ModelSummaryProxy modelSummaryProxy, Map<String, String> modelFieldMap) {
        return getEVFitFunctionParametersMap(customerSpace, yarnConfiguration, modelSummaryProxy, modelFieldMap, null);
    }

    public static Map<String, String> getEVFitFunctionParametersMap(CustomerSpace customerSpace,
            Configuration yarnConfiguration, ModelSummaryProxy modelSummaryProxy, Map<String, String> modelFieldMap,
            Map<String, String> fitFunctionParametersMapInContext) {
        ScoreArtifactRetriever scoreArtifactRetriever = new ScoreArtifactRetriever(modelSummaryProxy,
                yarnConfiguration);
        Map<String, String> fitFunctionParametersMap = new HashMap<>();
        modelFieldMap.entrySet().stream().forEach(entry -> {
            String modelId = entry.getKey();
            boolean isEV = ScoreResultField.ExpectedRevenue.displayName.equals(entry.getValue());
            String fitFunctionParameters = null;
            if (MapUtils.isNotEmpty(fitFunctionParametersMapInContext)
                    && StringUtils.isNotBlank(fitFunctionParametersMapInContext.get(modelId))) {
                fitFunctionParameters = fitFunctionParametersMapInContext.get(modelId);
            } else {
                fitFunctionParameters = scoreArtifactRetriever.getFitFunctionParameters(customerSpace, modelId, isEV);
            }

            if (fitFunctionParameters != null) {
                log.info(String.format("getEVFitFunctionParametersMap - modelId = %s, fitFunctionParameters = %s",
                        modelId, JsonUtils.serialize(fitFunctionParameters)));

                fitFunctionParametersMap.put(modelId, fitFunctionParameters);
            }
        });
        return fitFunctionParametersMap;
    }

    public static Map<String, Map<ScoreDerivationType, ScoreDerivation>> getScoreDerivationMap(
            CustomerSpace customerSpace, Configuration yarnConfiguration, ModelSummaryProxy modelSummaryProxy,
            Map<String, String> modelFieldMap, String fieldNameForEVIdentification, boolean loadOnlyForEVModel) {
        ScoreArtifactRetriever scoreArtifactRetriever = new ScoreArtifactRetriever(modelSummaryProxy,
                yarnConfiguration);
        Map<String, Map<ScoreDerivationType, ScoreDerivation>> scoreDerivationMap = new HashMap<>();
        modelFieldMap.entrySet().stream().forEach(entry -> {
            String modelId = entry.getKey();
            boolean isEV = fieldNameForEVIdentification.equals(entry.getValue());
            if (!loadOnlyForEVModel || isEV) {
                String scoreDerivationStr = scoreArtifactRetriever.getScoreDerivation(customerSpace, modelId, isEV);

                Map<ScoreDerivationType, ScoreDerivation> scoreDerivationInfo = new HashMap<>();

                if (isEV) {
                    EVScoreDerivation evScoreDerivation = JsonUtils.deserialize(scoreDerivationStr,
                            EVScoreDerivation.class);

                    scoreDerivationInfo.put(ScoreDerivationType.EV, evScoreDerivation.getEVScoreDerivation());
                    scoreDerivationInfo.put(ScoreDerivationType.PROBABILITY,
                            evScoreDerivation.getProbabilityScoreDerivation());
                    scoreDerivationInfo.put(ScoreDerivationType.REVENUE, evScoreDerivation.getRevenueScoreDerivation());
                } else {
                    ScoreDerivation scoreDerivation = JsonUtils.deserialize(scoreDerivationStr, ScoreDerivation.class);
                    scoreDerivationInfo.put(ScoreDerivationType.PROBABILITY, scoreDerivation);

                }

                log.info(String.format("getScoreDerivationMap - modelId = %s, scoreDerivationInfo = %s", modelId,
                        JsonUtils.serialize(scoreDerivationInfo)));
                scoreDerivationMap.put(modelId, scoreDerivationInfo);

            }
        });
        return scoreDerivationMap;
    }

    public static Map<String, String> getScoreFieldsMap(List<RatingModelContainer> allContainers) {
        Map<String, String> originalScoreFieldsMap;
        originalScoreFieldsMap = new HashMap<>();
        List<RatingModelContainer> containers = getModelContainers(allContainers);
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

    public static Map<String, String> getTargetScoreFiDerivationPaths(CustomerSpace customerSpace,
            Configuration yarnConfiguration, ModelSummaryProxy modelSummaryProxy, Map<String, String> scoreFieldMap) {
        ScoreArtifactRetriever retriever = new ScoreArtifactRetriever(modelSummaryProxy, yarnConfiguration);
        Map<String, String> targetScoreDerivationPaths = new HashMap<String, String>();
        scoreFieldMap.forEach((modelId, value) -> {
            String path = retriever.getTargetScoreDerivationPath(customerSpace, modelId);
            targetScoreDerivationPaths.put(modelId, path);
        });
        return targetScoreDerivationPaths;
    }

    private static List<RatingModelContainer> getModelContainers(List<RatingModelContainer> allContainers) {
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
