package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

import com.latticeengines.common.exposed.util.NamingUtils;

public class CalculatePredictedRevenuePercentileDataFlowConfiguration extends BaseScoringDataFlowStepConfiguration {

    public CalculatePredictedRevenuePercentileDataFlowConfiguration() {
        setBeanName("calculatePredictedRevenuePercentile");
        setTargetTableName(NamingUtils.uuid("calculatePredictedRevenuePercentile"));
    }
}
