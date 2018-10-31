package com.latticeengines.scoring.workflow.steps;

import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculatePredictedRevenuePercentileParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.CalculatePredictedRevenuePercentileDataFlowConfiguration;

@Component("calculatePredictedRevenuePercentileDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CalculatePredictedRevenuePercentileDataFlow
        extends AbstractCalculateRevenuePercentileDataFlow<CalculatePredictedRevenuePercentileDataFlowConfiguration> {

    @Override
    String getRevenueFieldName() {
        return ScoreResultField.PredictedRevenue.displayName;
    }

    @Override
    void initAndSetDataFlowParam(String inputTableName, String modelGuidField, int percentileLowerBound,
            int percentileUpperBound, Map<String, String> originalScoreFieldMap) {
        CalculatePredictedRevenuePercentileParameters params = new CalculatePredictedRevenuePercentileParameters();
        params.setInputTableName(inputTableName);
        params.setPercentileFieldName(ScoreResultField.PredictedRevenuePercentile.displayName);
        params.setModelGuidField(modelGuidField);
        params.setPercentileLowerBound(percentileLowerBound);
        params.setPercentileUpperBound(percentileUpperBound);

        if (MapUtils.isNotEmpty(originalScoreFieldMap)) {
            params.setOriginalScoreFieldMap(originalScoreFieldMap);
        }
        configuration.setDataFlowParams(params);
    }
}
