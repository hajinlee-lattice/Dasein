package com.latticeengines.domain.exposed.datacloud.match.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class DplusMatchConfig {

    @JsonProperty("BaseRule")
    private DplusMatchRule baseRule;

    @JsonProperty("SpecialRules")
    private List<SpeicalRule> specialRules;

    // for jackson
    private DplusMatchConfig(){}

    public DplusMatchConfig(DplusMatchRule baseRule) {
        this.baseRule = baseRule;
    }

    public DplusMatchRule getBaseRule() {
        return baseRule;
    }

    private void setBaseRule(DplusMatchRule baseRule) {
        this.baseRule = baseRule;
    }

    public List<SpeicalRule> getSpecialRules() {
        return specialRules;
    }

    private void setSpecialRules(List<SpeicalRule> specialRules) {
        this.specialRules = specialRules;
    }

    void addSpecialRule(SpeicalRule newRule) {
        List<SpeicalRule> sRules = getSpecialRules();
        if (CollectionUtils.isEmpty(sRules)) {
            sRules = new ArrayList<>();
        } else {
            sRules = new ArrayList<>(sRules);
        }
        sRules.add(newRule);
        setSpecialRules(sRules);
    }

    public DplusMatchConfigWhen when(MatchKey matchKey, Collection<String> values) {
        SpeicalRule predicate = new SpeicalRule();
        predicate.setMatchKey(matchKey);
        predicate.setAllowedValues(values);
        return new DplusMatchConfigWhen(this, predicate);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonAutoDetect( //
            fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE //
    )
    public static class SpeicalRule {

        // Input Predicate:
        @JsonProperty("MatchKey")
        private MatchKey matchKey;

        // the values will be standardized (e.g. converted to country code) then compare
        @JsonProperty("AllowedValues")
        private Collection<String> allowedValues;

        // Special Rule:
        @JsonProperty("SpecialRule")
        private DplusMatchRule specialRule;

        public MatchKey getMatchKey() {
            return matchKey;
        }

        private void setMatchKey(MatchKey matchKey) {
            this.matchKey = matchKey;
        }

        public Collection<String> getAllowedValues() {
            return allowedValues;
        }

        private void setAllowedValues(Collection<String> allowedValues) {
            this.allowedValues = allowedValues;
        }

        public DplusMatchRule getSpecialRule() {
            return specialRule;
        }

        void setSpecialRule(DplusMatchRule specialRule) {
            this.specialRule = specialRule;
        }
    }

}
