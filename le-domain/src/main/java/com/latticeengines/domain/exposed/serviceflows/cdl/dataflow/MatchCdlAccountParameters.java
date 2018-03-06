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

    @JsonProperty("match_fields")
    private List<String> matchFields;

    public MatchCdlAccountParameters(String inputTable, String accountTable) {
        this.inputTable = inputTable;
        this.accountTable = accountTable;
    }

    public List<String> getMatchFields() {
        return matchFields;
    }

    public void setMatchField(List<String> matchFields) {
        this.matchFields = matchFields;
    }

    public boolean isDedupe() {
        return dedupe;
    }

    public void setDedupe(boolean dedupe) {
        this.dedupe = dedupe;
    }

}
