package com.latticeengines.domain.exposed.workflow;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.CleanupByUploadWrapperConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.ConvertBatchStoreStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({
        @Type(value = BaseProcessEntityStepConfiguration.class, name = "BaseProcessEntityStepConfiguration"),
        @Type(value = ConvertBatchStoreStepConfiguration.class, name = "ConvertBatchStoreStepConfiguration"),
        @Type(value = CleanupByUploadWrapperConfiguration.class, name = "CleanupByUploadWrapperConfiguration"), })
public class BaseWrapperStepConfiguration extends BaseStepConfiguration {

    @NotNull
    @JsonProperty("customer_space")
    private CustomerSpace customerSpace;

    @JsonProperty("pod_id")
    private String podId = "";

    @JsonProperty("phase")
    private Phase phase = Phase.PRE_PROCESSING;

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    public String getPodId() {
        return podId;
    }

    public void setPodId(String podId) {
        this.podId = podId;
    }

    public Phase getPhase() {
        return phase;
    }

    public void setPhase(Phase phase) {
        this.phase = phase;
    }

    public enum Phase {
        PRE_PROCESSING, POST_PROCESSING
    }

}
