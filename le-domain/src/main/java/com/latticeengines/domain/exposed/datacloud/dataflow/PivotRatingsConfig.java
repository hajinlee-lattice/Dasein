package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class PivotRatingsConfig extends TransformerConfig {

    @JsonProperty("id_attrs_map")
    private Map<String, String> idAttrsMap; // model id to engine id mapping

    @JsonProperty("rule_source_idx")
    private Integer ruleSourceIdx;

    @JsonProperty("ai_source_idx")
    private Integer aiSourceIdx;

    @JsonProperty("inactive_source_idx")
    private Integer inactiveSourceIdx;

    @JsonProperty("ev_model_ids")
    private List<String> evModelIds;

    @JsonProperty("ai_model_ids")
    private List<String> aiModelIds;

    @JsonProperty("inactive_engines")
    private List<String> inactiveEngines;

    public Map<String, String> getIdAttrsMap() {
        return idAttrsMap;
    }

    public void setIdAttrsMap(Map<String, String> idAttrsMap) {
        this.idAttrsMap = idAttrsMap;
    }

    public Integer getRuleSourceIdx() {
        return ruleSourceIdx;
    }

    public void setRuleSourceIdx(Integer ruleSourceIdx) {
        this.ruleSourceIdx = ruleSourceIdx;
    }

    public Integer getAiSourceIdx() {
        return aiSourceIdx;
    }

    public void setAiSourceIdx(Integer aiSourceIdx) {
        this.aiSourceIdx = aiSourceIdx;
    }

    public Integer getInactiveSourceIdx() {
        return inactiveSourceIdx;
    }

    public void setInactiveSourceIdx(Integer inactiveSourceIdx) {
        this.inactiveSourceIdx = inactiveSourceIdx;
    }

    public List<String> getEvModelIds() {
        return evModelIds;
    }

    public void setEvModelIds(List<String> evModelIds) {
        this.evModelIds = evModelIds;
    }

    public List<String> getAiModelIds() {
        return aiModelIds;
    }

    public void setAiModelIds(List<String> aiModelIds) {
        this.aiModelIds = aiModelIds;
    }

    public List<String> getInactiveEngines() {
        return inactiveEngines;
    }

    public void setInactiveEngines(List<String> inactiveEngines) {
        this.inactiveEngines = inactiveEngines;
    }
}
