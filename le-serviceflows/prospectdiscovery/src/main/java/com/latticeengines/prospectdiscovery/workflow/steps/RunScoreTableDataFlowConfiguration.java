package com.latticeengines.prospectdiscovery.workflow.steps;

import com.latticeengines.domain.exposed.serviceflows.core.steps.DataFlowStepConfiguration;

public class RunScoreTableDataFlowConfiguration extends DataFlowStepConfiguration {
    
    private String[] accountMasterAndPath;
    private String scoreResult;
    private String uniqueKeyColumn;
    
    public RunScoreTableDataFlowConfiguration() {
        setBeanName("createScoreTable");
        setTargetTableName("CreateScoreTable");
    }

    public String[] getAccountMasterAndPath() {
        return accountMasterAndPath;
    }

    public void setAccountMasterNameAndPath(String[] accountMasterAndPath) {
        this.accountMasterAndPath = accountMasterAndPath;
    }

    public String getScoreResult() {
        return scoreResult;
    }

    public void setScoreResult(String scoreResult) {
        this.scoreResult = scoreResult;
    }

    public String getUniqueKeyColumn() {
        return uniqueKeyColumn;
    }

    public void setUniqueKeyColumn(String uniqueKeyColumn) {
        this.uniqueKeyColumn = uniqueKeyColumn;
    }
    
    
}
