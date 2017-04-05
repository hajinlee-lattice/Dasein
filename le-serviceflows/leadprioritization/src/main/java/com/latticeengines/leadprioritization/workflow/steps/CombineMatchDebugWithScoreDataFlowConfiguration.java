package com.latticeengines.leadprioritization.workflow.steps;

import java.util.UUID;

import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;

public class CombineMatchDebugWithScoreDataFlowConfiguration extends DataFlowStepConfiguration {

    public CombineMatchDebugWithScoreDataFlowConfiguration() {
        setBeanName("combineMatchDebugWithScore");
        setTargetTableName("CombineMatchDebugWithScore_" + UUID.randomUUID().toString());
    }

}
