package com.latticeengines.leadprioritization.workflow.steps;

import java.util.UUID;

import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;

public class CombineInputTableWithScoreDataFlowConfiguration extends DataFlowStepConfiguration {
    public CombineInputTableWithScoreDataFlowConfiguration() {
        setBeanName("combineInputTableWithScore");
        setTargetTableName("CombineInputTableWithScore_" + UUID.randomUUID().toString());
    }
}
