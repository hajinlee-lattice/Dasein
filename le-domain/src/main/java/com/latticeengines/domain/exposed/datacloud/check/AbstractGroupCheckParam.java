package com.latticeengines.domain.exposed.datacloud.check;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class AbstractGroupCheckParam extends CheckParam {

    @JsonProperty("GroupByFields")
    private List<String> groupByFields;

    public List<String> getGroupByFields() {
        return groupByFields;
    }

    public void setGroupByFields(List<String> groupByFields) {
        this.groupByFields = groupByFields;
    }
}
