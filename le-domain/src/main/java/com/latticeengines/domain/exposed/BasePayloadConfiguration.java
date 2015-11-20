package com.latticeengines.domain.exposed;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.HasName;

public class BasePayloadConfiguration implements HasName {

    private String name;
    private CustomerSpace customerSpace;

    @Override
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("customer_space")
    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    @JsonProperty("customer_space")
    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
