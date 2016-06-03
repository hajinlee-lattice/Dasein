package com.latticeengines.domain.exposed.modeling.review;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RuleRemediationEnablement {

    private Map<DataRuleName, Boolean> ruleEnablements;

    @JsonProperty
    public Map<DataRuleName, Boolean> getRuleEnablements() {
        return ruleEnablements;
    }

    @JsonProperty
    public void setRuleEnablements(Map<DataRuleName, Boolean> ruleEnablements) {
        this.ruleEnablements = ruleEnablements;
    }


}
