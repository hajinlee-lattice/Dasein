package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class UpdateStatsObjectsConfiguration extends BaseStepConfiguration {

    @JsonProperty("customer_space")
    @NotNull
    private CustomerSpace customerSpace;

    @JsonProperty("data_collection_name")
    @NotNull
    private String dataCollectionName;

    public CustomerSpace getCustomerSpace() {
        return this.customerSpace;
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    public String getDataCollectionName() {
        return dataCollectionName;
    }

    public void setDataCollectionName(String dataCollectionName) {
        this.dataCollectionName = dataCollectionName;
    }
}
