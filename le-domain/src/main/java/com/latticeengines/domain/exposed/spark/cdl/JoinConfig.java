package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class JoinConfig extends SparkJobConfig {

    public static final String NAME = "join";

    @JsonProperty("ParentId")
    private String parentId;

    @JsonProperty("ParentSrcIdx")
    private Integer parentSrcIdx; // default 1 (the second input)

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public Integer getParentSrcIdx() {
        return parentSrcIdx;
    }

    public void setParentSrcIdx(Integer parentSrcIdx) {
        this.parentSrcIdx = parentSrcIdx;
    }

}
