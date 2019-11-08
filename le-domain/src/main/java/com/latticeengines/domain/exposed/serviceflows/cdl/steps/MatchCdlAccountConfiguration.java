package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

public class MatchCdlAccountConfiguration extends BaseCDLDataFlowStepConfiguration {

    private String matchInputTableName;
    private String matchAccountIdColumn;
    private boolean mapToLatticeAccount;
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

    public void setMapToLatticeAccount(boolean mapToLatticeAccount) {
        this.mapToLatticeAccount = mapToLatticeAccount;
    }

    public boolean isMapToLatticeAccount() {
        return mapToLatticeAccount;
    }

}
