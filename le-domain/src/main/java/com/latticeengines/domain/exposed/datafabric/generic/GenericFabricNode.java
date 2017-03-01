package com.latticeengines.domain.exposed.datafabric.generic;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;

public class GenericFabricNode implements HasName {

    @JsonProperty("Name")
    private String name;
    @JsonProperty("TotalCount")
    private long totalCount = -1;
    @JsonProperty("Count")
    private long count = 0;
    @JsonProperty("Message")
    private String message;

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

}
