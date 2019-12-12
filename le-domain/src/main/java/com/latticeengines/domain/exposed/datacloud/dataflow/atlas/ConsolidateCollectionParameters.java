package com.latticeengines.domain.exposed.datacloud.dataflow.atlas;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;

public class ConsolidateCollectionParameters extends TransformationFlowParameters {

    @JsonProperty("group_by")
    private List<String> groupBy;

    @JsonProperty("sort_by")
    private String sortBy;

    public List<String> getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(List<String> groupBy) {
        this.groupBy = groupBy;
    }

    public String getSortBy() {
        return sortBy;
    }

    public void setSortBy(String sortBy) {
        this.sortBy = sortBy;
    }

}
