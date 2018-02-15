package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

import java.util.UUID;

public class CombineMatchDebugWithScoreDataFlowConfiguration extends BaseScoringDataFlowStepConfiguration {

    public CombineMatchDebugWithScoreDataFlowConfiguration() {
        setBeanName("combineMatchDebugWithScore");
        setTargetTableName("CombineMatchDebugWithScore_" + UUID.randomUUID().toString());
    }

}
