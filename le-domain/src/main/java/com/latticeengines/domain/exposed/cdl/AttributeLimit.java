package com.latticeengines.domain.exposed.cdl;

import org.codehaus.jackson.annotate.JsonProperty;

public class AttributeLimit {
    @JsonProperty("account_attribute_quota_limit")
    private Integer accountAttributeQuotaLimit;

    @JsonProperty("contact_attribute_quota_limit")
    private Integer contactAttributeQuotaLimit;

    public Integer getAccountAttributeQuotaLimit() {
        return accountAttributeQuotaLimit;
    }

    public void setAccountAttributeQuotaLimit(Integer accountAttributeQuotaLimit) {
        this.accountAttributeQuotaLimit = accountAttributeQuotaLimit;
    }

    public Integer getContactAttributeQuotaLimit() {
        return contactAttributeQuotaLimit;
    }

    public void setContactAttributeQuotaLimit(Integer contactAttributeQuotaLimit) {
        this.contactAttributeQuotaLimit = contactAttributeQuotaLimit;
    }
}
