package com.latticeengines.domain.exposed.datacloud.transformation.step;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TargetTable {

    @JsonProperty("NamePrefix")
    private String namePrefix;

    @JsonProperty("ExpandBucketedAttrs")
    private Boolean expandBucketedAttrs;

    @JsonProperty("PrimaryKey")
    private String primaryKey;

    @JsonProperty("LastModifiedKey")
    private String lastModifiedKey;

    @JsonIgnore
    private CustomerSpace customerSpace;

    public String getNamePrefix() {
        return namePrefix;
    }

    public void setNamePrefix(String namePrefix) {
        this.namePrefix = namePrefix;
    }

    public Boolean getExpandBucketedAttrs() {
        return expandBucketedAttrs;
    }

    public void setExpandBucketedAttrs(Boolean expandBucketedAttrs) {
        this.expandBucketedAttrs = expandBucketedAttrs;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getLastModifiedKey() {
        return lastModifiedKey;
    }

    public void setLastModifiedKey(String lastModifiedKey) {
        this.lastModifiedKey = lastModifiedKey;
    }

    @JsonIgnore
    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    @JsonIgnore
    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    @JsonProperty("CustomerSpace")
    private String getCustomerSpaceAsString() {
        return customerSpace.toString();
    }

    @JsonProperty("CustomerSpace")
    private void setCustomerSpaceViaString(String customerSpace) {
        this.customerSpace = CustomerSpace.parse(customerSpace);
    }
}
