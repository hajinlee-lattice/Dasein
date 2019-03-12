package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ApsGenerationStepConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;


@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "name")
@JsonSubTypes({ //
        @JsonSubTypes.Type(value = ApsGenerationStepConfiguration.class, name = "ApsGenerationStepConfiguration") //
})
public class SparkScriptStepConfiguration extends BaseStepConfiguration {

    @JsonProperty("customer")
    private String customer;

    public String getCustomer() {
        return customer;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }

}
