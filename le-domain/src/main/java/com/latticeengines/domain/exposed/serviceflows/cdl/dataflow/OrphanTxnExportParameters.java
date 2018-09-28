package com.latticeengines.domain.exposed.serviceflows.cdl.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class OrphanTxnExportParameters extends DataFlowParameters {

    @JsonProperty("account_table")
    @SourceTableName
    public String accountTable;

    @JsonProperty("product_table")
    @SourceTableName
    public String productTable;

    @JsonProperty("txn_table")
    @SourceTableName
    public String txnTable;

    public OrphanTxnExportParameters() {
    }

    public OrphanTxnExportParameters(String accountTable, String productTable, String txnTable) {
        this.accountTable = accountTable;
        this.productTable = productTable;
        this.txnTable = txnTable;
    }

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

    public String getTxnTable() {
        return txnTable;
    }

    public void setTxnTable(String txnTable) {
        this.txnTable = txnTable;
    }
}
