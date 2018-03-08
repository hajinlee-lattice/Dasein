package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

public class MatchCdlAccountConfiguration extends BaseCDLDataFlowStepConfiguration {

    public MatchCdlAccountConfiguration() {
        setBeanName("matchCdlAccountFlow");
    }

    private String matchInputTableName;

    public String getMatchInputTableName() {
        return matchInputTableName;
    }

    public void setMatchInputTableName(String matchInputTableName) {
        this.matchInputTableName = matchInputTableName;
    }

}
