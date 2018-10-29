package com.latticeengines.domain.exposed.serviceflows.cdl.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class OrphanTransactionExportParameters extends DataFlowParameters {

    @JsonProperty("AccountTable")
    @SourceTableName
    public String accountTable;

    @JsonProperty("ProductTable")
    @SourceTableName
    public String productTable;

    @JsonProperty("TransactionTable")
    @SourceTableName
    public String transactionTable;

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
}
