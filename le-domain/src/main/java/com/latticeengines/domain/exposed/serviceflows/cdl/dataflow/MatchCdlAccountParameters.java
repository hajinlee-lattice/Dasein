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

    @JsonProperty("input_match_fields")
    private List<String> inputMatchFields;

    @JsonProperty("account_match_fields")
    private List<String> accountMatchFields;

    @JsonProperty("has_account_id")
    private boolean hasAccountId;

    @JsonProperty("rename_id_only")
    private boolean renameIdOnly;

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

    public boolean isHasAccountId() {
        return this.hasAccountId;
    }

    public void setHasAccountId(boolean hasAccountId) {
        this.hasAccountId = hasAccountId;
    }

    public boolean isRenameIdOnly() {
        return renameIdOnly;
    }

    public void setRenameIdOnly(boolean renameIdOnly) {
        this.renameIdOnly = renameIdOnly;
    }
}
