package com.latticeengines.scoring.workflow.steps;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.scoring.workflow.util.ScoreArtifactRetriever;

public class ExpectedRevenueDataFlowUtil {

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
                fitFunctionParametersMap.put(modelId, fitFunctionParameters);
            }
        });
        return fitFunctionParametersMap;
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
