package com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps;

import java.util.UUID;

import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

public class CombineMatchDebugWithScoreDataFlowConfiguration extends BaseLPDataFlowStepConfiguration {

    public CombineMatchDebugWithScoreDataFlowConfiguration() {
        setBeanName("combineMatchDebugWithScore");
        setTargetTableName("CombineMatchDebugWithScore_" + UUID.randomUUID().toString());
    }

    @Override
    public String getSwlib() {
        return SoftwareLibrary.Scoring.getName();
    }

}
