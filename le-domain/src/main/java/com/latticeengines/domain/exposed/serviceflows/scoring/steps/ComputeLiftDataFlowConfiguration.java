package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

import com.latticeengines.common.exposed.util.NamingUtils;

public class ComputeLiftDataFlowConfiguration extends BaseScoringDataFlowStepConfiguration {

    private String scoreField;

    public ComputeLiftDataFlowConfiguration() {
        setBeanName("computeLift");
        setTargetTableName(NamingUtils.uuid("ComputeLift"));
    }

    public String getScoreField() {
        return scoreField;
    }

    public void setScoreField(String scoreField) {
        this.scoreField = scoreField;
    }
}
