package com.latticeengines.domain.exposed.datacloud.transformation.step;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SourceTable {

    @JsonProperty("TableName")
    private String tableName;

    @JsonIgnore
    private CustomerSpace customerSpace;

    public SourceTable(String tableName, CustomerSpace customerSpace) {
        this.tableName = tableName;
        this.customerSpace = customerSpace;
    }

    // for jackson
    private SourceTable() {}

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
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
