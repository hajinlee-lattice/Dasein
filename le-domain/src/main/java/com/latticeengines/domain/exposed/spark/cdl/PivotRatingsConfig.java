package com.latticeengines.domain.exposed.spark.cdl;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class PivotRatingsConfig extends SparkJobConfig {

    public static final String NAME = "pivotRatings";

    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @JsonProperty("IdAttrsMap")
    private Map<String, String> idAttrsMap; // model id to engine id mapping

    @JsonProperty("RuleSourceIdx")
    private Integer ruleSourceIdx; // which input is rule based

    @JsonProperty("AiSourceIdx")
    private Integer aiSourceIdx; // which input is ai based

    @JsonProperty("InactiveSourceIdx")
    private Integer inactiveSourceIdx; // which input is inactive ratings

    @JsonProperty("EvModelIds")
    private List<String> evModelIds;

    @JsonProperty("AIModelIds")
    private List<String> aiModelIds;

    @JsonProperty("InactiveEngineIds")
    private List<String> inactiveEngineIds;

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

    public List<String> getInactiveEngineIds() {
        return inactiveEngineIds;
    }

    public void setInactiveEngineIds(List<String> inactiveEngineIds) {
        this.inactiveEngineIds = inactiveEngineIds;
    }
}
