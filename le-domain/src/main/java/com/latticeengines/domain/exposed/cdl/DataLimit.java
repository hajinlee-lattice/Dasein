package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DataLimit {

    @JsonProperty("account_data_quota_limit")
    private Long accountDataQuotaLimit;

    @JsonProperty("contact_data_quota_limit")
    private Long contactDataQuotaLimit;

    @JsonProperty("transaction_data_quota_limit")
    private Long transactionDataQuotaLimit;

    @JsonProperty("product_bundle_data_quota_limit")
    private Long productBundleDataQuotaLimit;

    @JsonProperty("product_sku_data_quota_limit")
    private Long productSkuDataQuotaLimit;

    public Long getAccountDataQuotaLimit() {
        return accountDataQuotaLimit;
    }

    public void setAccountDataQuotaLimit(Long accountDataQuotaLimit) {
        this.accountDataQuotaLimit = accountDataQuotaLimit;
    }

    public Long getContactDataQuotaLimit() {
        return contactDataQuotaLimit;
    }

    public void setContactDataQuotaLimit(Long contactDataQuotaLimit) {
        this.contactDataQuotaLimit = contactDataQuotaLimit;
    }

    public Long getTransactionDataQuotaLimit() {
        return transactionDataQuotaLimit;
    }

    public void setTransactionDataQuotaLimit(Long transactionDataQuotaLimit) {
        this.transactionDataQuotaLimit = transactionDataQuotaLimit;
    }

    public Long getProductBundleDataQuotaLimit() {
        return productBundleDataQuotaLimit;
    }

    public void setProductBundleDataQuotaLimit(Long productBundleDataQuotaLimit) {
        this.productBundleDataQuotaLimit = productBundleDataQuotaLimit;
    }

    public Long getProductSkuDataQuotaLimit() {
        return productSkuDataQuotaLimit;
    }

    public void setProductSkuDataQuotaLimit(Long productSkuDataQuotaLimit) {
        this.productSkuDataQuotaLimit = productSkuDataQuotaLimit;
    }
}
