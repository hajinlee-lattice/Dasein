package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

public class OrphanRecordExportConfiguration extends BaseCDLDataFlowStepConfiguration {

    private String txnTableName;
    private String accountTableName;
    private String productTableName;
    private String orphanRecordExportId;
    public OrphanRecordExportConfiguration() {
        setBeanName("OrphanTxnExportFlow");
    }

    public String getTxnTableName() {
        return txnTableName;
    }

    public void setTxnTableName(String txnTableName) {
        this.txnTableName = txnTableName;
    }

    public String getAccountTableName() {
        return accountTableName;
    }

    public void setAccountTableName(String accountTableName) {
        this.accountTableName = accountTableName;
    }

    public String getProductTableName() {
        return productTableName;
    }

    public void setProductTableName(String productTableName) {
        this.productTableName = productTableName;
    }

    public String getOrphanRecordExportId() {
        return orphanRecordExportId;
    }

    public void setOrphanRecordExportId(String orphanRecordExportId) {
        this.orphanRecordExportId = orphanRecordExportId;
    }

}
