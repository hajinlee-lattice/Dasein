package com.latticeengines.cdl.workflow.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class CalculateStatsStepConfiguration extends BaseStepConfiguration {

    @JsonProperty("master_table_name")
    private String masterTableName;

    @JsonProperty("customer_space")
    private CustomerSpace customerSpace;

    public String getMasterTableName() {
        return this.masterTableName;
    }

    public void setMasterTableName(String masterTableName) {
        this.masterTableName = masterTableName;
    }

    public CustomerSpace getCustomerSpace() {
        return this.customerSpace;
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }
}
