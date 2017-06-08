package com.latticeengines.leadprioritization.workflow.steps;

import java.util.UUID;

import com.latticeengines.domain.exposed.serviceflows.core.steps.DataFlowStepConfiguration;

public class CombineMatchDebugWithScoreDataFlowConfiguration extends DataFlowStepConfiguration {

    public CombineMatchDebugWithScoreDataFlowConfiguration() {
        setBeanName("combineMatchDebugWithScore");
        setTargetTableName("CombineMatchDebugWithScore_" + UUID.randomUUID().toString());
    }

}
