package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class CleanupConfig extends TransformerConfig {

    @JsonProperty("operation_type")
    private CleanupOperationType operationType;

    @JsonProperty("business_entity")
    private BusinessEntity businessEntity;

    @JsonProperty("base_joined_columns")
    private JoinedColumns baseJoinedColumns;

    @JsonProperty("delete_joined_columns")
    private JoinedColumns deleteJoinedColumns;

    public CleanupOperationType getOperationType() {
        return operationType;
    }

    public void setOperationType(CleanupOperationType operationType) {
        this.operationType = operationType;
    }

    public BusinessEntity getBusinessEntity() {
        return businessEntity;
    }

    public void setBusinessEntity(BusinessEntity businessEntity) {
        this.businessEntity = businessEntity;
    }

    public JoinedColumns getBaseJoinedColumns() {
        return baseJoinedColumns;
    }

    public void setBaseJoinedColumns(JoinedColumns baseJoinedColumns) {
        this.baseJoinedColumns = baseJoinedColumns;
    }

    public JoinedColumns getDeleteJoinedColumns() {
        return deleteJoinedColumns;
    }

    public void setDeleteJoinedColumns(JoinedColumns deleteJoinedColumns) {
        this.deleteJoinedColumns = deleteJoinedColumns;
    }

    public static class JoinedColumns {

        @JsonProperty("account_id")
        private String accountId;

        @JsonProperty("contact_id")
        private String contactId;

        @JsonProperty("product_id")
        private String productId;

        @JsonProperty("transaction_time")
        private String transactionTime;

        public String getAccountId() {
            return accountId;
        }

        public void setAccountId(String accountId) {
            this.accountId = accountId;
        }

        public String getContactId() {
            return contactId;
        }

        public void setContactId(String contactId) {
            this.contactId = contactId;
        }

        public String getProductId() {
            return productId;
        }

        public void setProductId(String productId) {
            this.productId = productId;
        }

        public String getTransactionTime() {
            return transactionTime;
        }

        public void setTransactionTime(String transactionTime) {
            this.transactionTime = transactionTime;
        }
    }

}
