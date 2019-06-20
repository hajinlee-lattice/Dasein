package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.NamingUtils;

public class CalculateExpectedRevenuePercentileDataFlowConfiguration
        extends BaseScoringDataFlowStepConfiguration {

    @JsonProperty("target_score_derivation")
    private boolean targetScoreDerivation;
    
    public CalculateExpectedRevenuePercentileDataFlowConfiguration() {
        setBeanName("calculateExpectedRevenuePercentile");
        setTargetTableName(NamingUtils.uuid("calculateExpectedRevenuePercentile"));
    }

    public boolean isTargetScoreDerivation() {
        return targetScoreDerivation;
    }

    public void setTargetScoreDerivation(boolean targetScoreDerivation) {
        this.targetScoreDerivation = targetScoreDerivation;
    }
}
