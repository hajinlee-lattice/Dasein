package com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class CommitEntityMatchConfiguration extends BaseStepConfiguration {

    @JsonProperty("customer_space")
    private CustomerSpace customerSpace;

    @JsonProperty("entity_list")
    private Set<String> entitySet;

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    public Set<String> getEntitySet() {
        return entitySet;
    }

    public void setEntitySet(Set<String> entitySet) {
        this.entitySet = entitySet;
    }

    public void addEntity(String entity) {
        if (this.entitySet == null) {
            this.entitySet = new HashSet<>();
        }
        this.entitySet.add(entity);
    }
}
