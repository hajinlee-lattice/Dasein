package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "name")
@JsonSubTypes({ //
        @JsonSubTypes.Type(value = ProcessMatchResultConfiguration.class, name = "ProcessMatchResultConfiguration") //
})
public class SparkJobStepConfiguration extends BaseStepConfiguration {

    @JsonProperty("customer")
    private String customer;

    public String getCustomer() {
        return customer;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }
}
