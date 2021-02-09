package com.latticeengines.domain.exposed.datacloud.match;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class InternalAccountLookupRequest {

    @JsonProperty("CustomerSpace")
    private String customerSpace;

    @JsonProperty("IndexName")
    private String indexName;


    @JsonProperty("LookupIdKey")
    private String lookupIdKey;

    @JsonProperty("LookupIdValue")
    private String lookupIdValue;

    public String getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(String customerSpace) {
        this.customerSpace = customerSpace;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getLookupIdKey() {
        return lookupIdKey;
    }

    public void setLookupIdKey(String lookupIdKey) {
        this.lookupIdKey = lookupIdKey;
    }

    public String getLookupIdValue() {
        return lookupIdValue;
    }

    public void setLookupIdValue(String lookupIdValue) {
        this.lookupIdValue = lookupIdValue;
    }
}
