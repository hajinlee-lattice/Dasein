package com.latticeengines.domain.exposed.spark.dcp;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dcp.DataReportMode;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class RollupDataReportConfig extends SparkJobConfig {

    public static final String NAME = "RollupDataReport";

    @JsonProperty("numTargets")
    private int numTargets;

    @JsonProperty("MatchedDunsAttr")
    private String matchedDunsAttr;

    // this records the owner id to input index
    @JsonProperty("IdToIndex")
    private Map<String, Integer> inputOwnerIdToIndex;

    // this records the owner id that need to update duns count table from source level
    // to the above
    @JsonProperty("OwnerIds")
    private List<String> updatedOwnerIds;

    // this records the parentId to its adjacent child node
    @JsonProperty("ParentIdToChildren")
    private Map<String, Set<String>> parentIdToChildren;

    @JsonProperty("Mode")
    private DataReportMode mode;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return numTargets;
    }

    public void setNumTargets(int numTargets) {
        this.numTargets = numTargets;
    }

    public String getMatchedDunsAttr() {
        return matchedDunsAttr;
    }

    public void setMatchedDunsAttr(String matchedDunsAttr) {
        this.matchedDunsAttr = matchedDunsAttr;
    }

    public Map<String, Integer> getInputOwnerIdToIndex() {
        return inputOwnerIdToIndex;
    }

    public void setInputOwnerIdToIndex(Map<String, Integer> inputOwnerIdToIndex) {
        this.inputOwnerIdToIndex = inputOwnerIdToIndex;
    }

    public List<String> getUpdatedOwnerIds() {
        return updatedOwnerIds;
    }

    public void setUpdatedOwnerIds(List<String> updatedOwnerIds) {
        this.updatedOwnerIds = updatedOwnerIds;
    }

    public Map<String, Set<String>> getParentIdToChildren() {
        return parentIdToChildren;
    }

    public void setParentIdToChildren(Map<String, Set<String>> parentIdToChildren) {
        this.parentIdToChildren = parentIdToChildren;
    }

    public DataReportMode getMode() {
        return mode;
    }

    public void setMode(DataReportMode mode) {
        this.mode = mode;
    }
}
