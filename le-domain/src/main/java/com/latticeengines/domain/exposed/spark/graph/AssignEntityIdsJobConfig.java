package com.latticeengines.domain.exposed.spark.graph;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class AssignEntityIdsJobConfig extends SparkJobConfig {

    public static final String NAME = "assignEntityIdsJob";

    // Sequence of (docV-idV) in descending order of confidence
    @JsonProperty("EdgeRank")
    private List<String> edgeRank;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return 3;
    }

    public List<String> getEdgeRank() {
        return edgeRank;
    }

    public void setEdgeRank(List<String> edgeRank) {
        this.edgeRank = edgeRank;
    }
}
