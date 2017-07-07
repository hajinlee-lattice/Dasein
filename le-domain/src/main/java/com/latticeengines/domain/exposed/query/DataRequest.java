package com.latticeengines.domain.exposed.query;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DataRequest {

    @JsonProperty("attributes")
    @ApiModelProperty(required = false, value = "List of attributes to return in addition to accountid, salesforceaccountid, latticeaccountid")
    private List<String> attributes = new ArrayList<>();

    @JsonProperty("account_ids")
    @ApiModelProperty(required = false, value = "Specify exactly which accounts to return")
    private List<String> accountIds = new ArrayList<>();

    public DataRequest() {
    }

    public List<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<String> attributes) {
        this.attributes = attributes;
    }

    public List<String> getAccountIds() {
        return accountIds;
    }

    public void setAccountIds(List<String> accountIds) {
        this.accountIds = accountIds;
    }
}
