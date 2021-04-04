package com.latticeengines.domain.exposed.spark.graph;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class AssignEntityIdsJobConfig extends SparkJobConfig {

    public static final String NAME = "assignEntityIdsJob";

    // Key is "docV-idV" and the value is confidence score
    // The higher the more confident that the pair is accurate
    @JsonProperty("MatchConfidenceScore")
    private Map<String, Integer> matchConfidenceScore;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return 3;
    }

    public Map<String, Integer> getMatchConfidenceScore() {
        return matchConfidenceScore;
    }

    public void setMatchConfidenceScore(Map<String, Integer> matchConfidenceScore) {
        this.matchConfidenceScore = matchConfidenceScore;
    }
}
