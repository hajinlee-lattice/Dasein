package com.latticeengines.domain.exposed.dcp.match;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class MatchRuleConfiguration {

    @JsonProperty(value = "baseRule", required = true)
    private MatchRule baseRule;

    @JsonProperty("specialRules")
    private List<MatchRule> specialRules;

    public MatchRule getBaseRule() {
        return baseRule;
    }

    public void setBaseRule(MatchRule baseRule) {
        this.baseRule = baseRule;
    }

    public List<MatchRule> getSpecialRules() {
        return specialRules;
    }

    public void setSpecialRules(List<MatchRule> specialRules) {
        this.specialRules = specialRules;
    }
}
