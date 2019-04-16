package com.latticeengines.domain.exposed.serviceflows.cdl.dataflow;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class OrphanTransactionExportParameters extends DataFlowParameters {

    @JsonProperty("AccountTable")
    @SourceTableName
    private String accountTable;

    @JsonProperty("ProductTable")
    @SourceTableName
    private String productTable;

    @JsonProperty("TransactionTable")
    @SourceTableName
    private String transactionTable;

    @JsonProperty("validatedColumns")
    private List<String> validatedColumns;

    public String getAccountTable() {
        return accountTable;
    }

    public void setAccountTable(String accountTable) {
        this.accountTable = accountTable;
    }

    public String getProductTable() {
        return productTable;
    }

    public void setProductTable(String productTable) {
        this.productTable = productTable;
    }

    public String getTransactionTable() {
        return transactionTable;
    }

    public void setTransactionTable(String transactionTable) {
        this.transactionTable = transactionTable;
    }

    public List<String> getValidatedColumns() {
        return validatedColumns;
    }

    public void setValidatedColumns(List<String> validatedColumns) {
        this.validatedColumns = validatedColumns;
    }
}
