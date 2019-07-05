package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.NamingUtils;

public class RecalculatePercentileScoreDataFlowConfiguration
        extends BaseScoringDataFlowStepConfiguration {

    @JsonProperty("target_score_derivation")
    private boolean targetScoreDerivation;
    
    public RecalculatePercentileScoreDataFlowConfiguration() {
        setBeanName("recalculatePercentileScore");
        setTargetTableName(NamingUtils.uuid("RecalculatePercentileScore"));
    }

    public boolean isTargetScoreDerivation() {
        return targetScoreDerivation;
    }

    public void setTargetScoreDerivation(boolean targetScoreDerivation) {
        this.targetScoreDerivation = targetScoreDerivation;
    }
    
}
