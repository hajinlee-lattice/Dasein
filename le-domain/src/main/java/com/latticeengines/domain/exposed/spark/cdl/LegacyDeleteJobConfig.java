package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class LegacyDeleteJobConfig extends SparkJobConfig {

    public static final String NAME = "legacyDeleteJob";

    @JsonProperty("operation_type")
    private CleanupOperationType operationType;

    @JsonProperty("business_entity")
    private BusinessEntity businessEntity;

    @JsonProperty("delete_source_idx")
    private Integer deleteSourceIdx;

    @JsonProperty("joined_columns")
    private JoinedColumns joinedColumns;

    @JsonProperty("source_columns")
    private JoinedColumns sourceColumns;

    @Override
    public String getName() {
        return NAME;
    }

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

    public Integer getDeleteSourceIdx() {
        return deleteSourceIdx;
    }

    public void setDeleteSourceIdx(Integer deleteSourceIdx) {
        this.deleteSourceIdx = deleteSourceIdx;
    }

    public JoinedColumns getJoinedColumns() {
        return joinedColumns;
    }

    public void setJoinedColumns(JoinedColumns joinedColumns) {
        this.joinedColumns = joinedColumns;
    }

    public JoinedColumns getSourceColumns() {
        return sourceColumns;
    }

    public void setSourceColumns(JoinedColumns sourceColumns) {
        this.sourceColumns = sourceColumns;
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
