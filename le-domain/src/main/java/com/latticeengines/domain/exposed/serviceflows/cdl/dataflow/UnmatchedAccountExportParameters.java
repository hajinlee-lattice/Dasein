package com.latticeengines.domain.exposed.serviceflows.cdl.dataflow;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class UnmatchedAccountExportParameters extends DataFlowParameters {

    @JsonProperty("AccountTable")
    @SourceTableName
    private String accountTable;

    @JsonProperty("validatedColumns")
    private List<String> validatedColumns;

    public String getAccountTable() {
        return accountTable;
    }

    public void setAccountTable(String accountTable) {
        this.accountTable = accountTable;
    }

    public List<String> getValidatedColumns() {
        return validatedColumns;
    }

    public void setValidatedColumns(List<String> validatedColumns) {
        this.validatedColumns = validatedColumns;
    }
}
