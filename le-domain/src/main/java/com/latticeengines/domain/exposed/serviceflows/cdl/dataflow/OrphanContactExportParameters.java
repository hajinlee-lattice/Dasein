package com.latticeengines.domain.exposed.serviceflows.cdl.dataflow;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class OrphanContactExportParameters extends DataFlowParameters {

    @JsonProperty("AccountTable")
    @SourceTableName
    private String accountTable;

    @JsonProperty("ContactTable")
    @SourceTableName
    private String contactTable;

    @JsonProperty("validatedColumns")
    private List<String> validatedColumns;

    public String getAccountTable() {
        return accountTable;
    }

    public void setAccountTable(String accountTable) {
        this.accountTable = accountTable;
    }

    public String getContactTable() {
        return contactTable;
    }

    public void setContactTable(String contactTable) {
        this.contactTable = contactTable;
    }

    public List<String> getValidatedColumns() {
        return validatedColumns;
    }

    public void setValidatedColumns(List<String> validatedColumns) {
        this.validatedColumns = validatedColumns;
    }

}
