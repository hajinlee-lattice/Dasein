package com.latticeengines.scoring.dataflow;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculatePredictedRevenuePercentileParameters;

@Component("calculatePredictedRevenuePercentile")
public class CalculatePredictedRevenuePercentile
        extends AbstractCalculateRevenuePercentile<CalculatePredictedRevenuePercentileParameters> {

    @Override
    protected void parseParamAndSetFields(CalculatePredictedRevenuePercentileParameters parameters) {
        inputTableName = parameters.getInputTableName();
        percentileFieldName = parameters.getPercentileFieldName();
        modelGuidFieldName = parameters.getModelGuidField();
        originalScoreFieldMap = parameters.getOriginalScoreFieldMap();
        minPct = parameters.getPercentileLowerBound();
        maxPct = parameters.getPercentileUpperBound();
    }
}
