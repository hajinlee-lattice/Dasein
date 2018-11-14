package com.latticeengines.scoring.dataflow;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculateExpectedRevenuePercentileParameters;

@Component("calculateExpectedRevenuePercentile")
public class CalculateExpectedRevenuePercentile
        extends AbstractCalculateRevenuePercentile<CalculateExpectedRevenuePercentileParameters> {

    @Override
    protected void parseParamAndSetFields(CalculateExpectedRevenuePercentileParameters parameters) {
        inputTableName = parameters.getInputTableName();
        percentileFieldName = parameters.getPercentileFieldName();
        modelGuidFieldName = parameters.getModelGuidField();
        originalScoreFieldMap = parameters.getOriginalScoreFieldMap();
        minPct = parameters.getPercentileLowerBound();
        maxPct = parameters.getPercentileUpperBound();
        renameNewPercentileToStandardPercentileField = true;
    }
}
