package com.latticeengines.domain.exposed.serviceflows.cdl.dataflow;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class MatchCdlAccountParameters extends DataFlowParameters {

    @JsonProperty("input_table")
    @SourceTableName
    public String inputTable;

    @JsonProperty("account_table")
    @SourceTableName
    public String accountTable;

    @JsonProperty("dedupe")
    private boolean dedupe;

    @JsonProperty("input_match_fields")
    private List<String> inputMatchFields;

    @JsonProperty("account_match_fields")
    private List<String> accountMatchFields;

    @JsonProperty("right_join")
    private boolean rightJoin;

    public MatchCdlAccountParameters() {
    }

    public MatchCdlAccountParameters(String inputTable, String accountTable) {
        this.inputTable = inputTable;
        this.accountTable = accountTable;
    }

    public List<String> getInputMatchFields() {
        return inputMatchFields;
    }

    public void setInputMatchFields(List<String> inputMatchFields) {
        this.inputMatchFields = inputMatchFields;
    }

    public List<String> getAccountMatchFields() {
        return accountMatchFields;
    }

    public void setAccountMatchFields(List<String> accountMatchFields) {
        this.accountMatchFields = accountMatchFields;
    }

    public boolean isDedupe() {
        return dedupe;
    }

    public void setDedupe(boolean dedupe) {
        this.dedupe = dedupe;
    }

    public boolean isRightJoin() {
        return this.rightJoin;
    }

    public void setRightJoin(boolean rightJoin) {
        this.rightJoin = rightJoin;
    }
}
