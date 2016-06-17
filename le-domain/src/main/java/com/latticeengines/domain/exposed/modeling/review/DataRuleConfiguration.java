package com.latticeengines.domain.exposed.modeling.review;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DataRuleConfiguration {

    private Set<DataRule> ruleEnablements;

    @JsonProperty
    public Set<DataRule> getRuleEnablements() {
        return ruleEnablements;
    }

    @JsonProperty
    public void setRuleEnablements(Set<DataRule> ruleEnablements) {
        this.ruleEnablements = ruleEnablements;
    }

}
