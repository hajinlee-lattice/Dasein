package com.latticeengines.leadprioritization.workflow.steps;

import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;

public class CombineInputTableWithScoreDataFlowConfiguration extends DataFlowStepConfiguration {

    private String scoreResultTableName;

    private String inputTableName;

    public CombineInputTableWithScoreDataFlowConfiguration() {
        setBeanName("combineInputTableWithScore");
        setName("CombineInputTableWithScore");
        setTargetPath("/CombineInputTableWithScore");
    }

    public String getScoreResultTableName() {
        return scoreResultTableName;
    }

    public void setScoreResultTableName(String scoreResultTableName) {
        this.scoreResultTableName = scoreResultTableName;
    }

    public String getInputTableName() {
        return inputTableName;
    }

    public void setInputTableName(String inputTableName) {
        this.inputTableName = inputTableName;
    }
}
