package com.latticeengines.scoring.workflow.steps;

import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculatePredictedRevenuePercentileParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.CalculatePredictedRevenuePercentileDataFlowConfiguration;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;

@Component("calculatePredictedRevenuePercentileDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CalculatePredictedRevenuePercentileDataFlow
        extends AbstractCalculateRevenuePercentileDataFlow<CalculatePredictedRevenuePercentileDataFlowConfiguration> {
    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Override
    String getRevenueFieldName() {
        return ScoreResultField.PredictedRevenue.displayName;
    }

    @Override
    boolean shouldLoadNormalizationRatio() {
        return false;
    }

    @Override
    void initAndSetDataFlowParam(String inputTableName, String modelGuidField, int percentileLowerBound,
            int percentileUpperBound, Map<String, String> originalScoreFieldMap,
            Map<String, Double> normalizationRatioMap) {
        CalculatePredictedRevenuePercentileParameters params = new CalculatePredictedRevenuePercentileParameters();
        params.setCustomerSpace(configuration.getCustomerSpace());
        params.setInputTableName(inputTableName);
        params.setPercentileFieldName(ScoreResultField.PredictedRevenuePercentile.displayName);
        params.setModelGuidField(modelGuidField);
        params.setPercentileLowerBound(percentileLowerBound);
        params.setPercentileUpperBound(percentileUpperBound);

        if (MapUtils.isNotEmpty(originalScoreFieldMap)) {
            params.setOriginalScoreFieldMap(originalScoreFieldMap);
        }

        ExpectedRevenueDataFlowUtil.getEVFitFunctionParametersMap( //
                configuration.getCustomerSpace(), yarnConfiguration, modelSummaryProxy, originalScoreFieldMap);

        configuration.setDataFlowParams(params);
    }
}
