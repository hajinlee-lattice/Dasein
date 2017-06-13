package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class UpdateStatsObjectsConfiguration extends BaseStepConfiguration {

    @JsonProperty("customer_space")
    private CustomerSpace customerSpace;

    @JsonProperty("data_collection_type")
    @NotNull
    private DataCollectionType dataCollectionType;

    public CustomerSpace getCustomerSpace() {
        return this.customerSpace;
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    public DataCollectionType getDataCollectionType() {
        return dataCollectionType;
    }

    public void setDataCollectionType(DataCollectionType dataCollectionType) {
        this.dataCollectionType = dataCollectionType;
    }

}
