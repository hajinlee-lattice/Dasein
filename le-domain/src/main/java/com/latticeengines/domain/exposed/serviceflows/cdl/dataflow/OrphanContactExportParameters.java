package com.latticeengines.domain.exposed.serviceflows.cdl.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class OrphanContactExportParameters extends DataFlowParameters {

    @JsonProperty("AccountTable")
    @SourceTableName
    public String accountTable;

    @JsonProperty("ContactTable")
    @SourceTableName
    public String contactTable;

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
}
