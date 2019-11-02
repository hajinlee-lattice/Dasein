package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

public class MatchCdlAccountConfiguration extends BaseCDLDataFlowStepConfiguration {

    private String matchInputTableName;
    private String matchAccountIdColumn;
    public MatchCdlAccountConfiguration() {
        setBeanName("matchCdlAccountFlow");
    }

    public String getMatchInputTableName() {
        return matchInputTableName;
    }

    public void setMatchInputTableName(String matchInputTableName) {
        this.matchInputTableName = matchInputTableName;
    }

    public String getMatchAccountIdColumn() {
        return this.matchAccountIdColumn;
    }

    public void setMatchAccountIdColumn(String matchAccountIdColumn) {
        this.matchAccountIdColumn = matchAccountIdColumn;
    }
}
