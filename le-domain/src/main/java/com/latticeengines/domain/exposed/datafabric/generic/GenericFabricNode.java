package com.latticeengines.domain.exposed.datafabric.generic;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;

public class GenericFabricNode implements HasName {

    @JsonProperty("Name")
    private String name;
    @JsonProperty("TotalCount")
    private long totalCount = -1;
    @JsonProperty("FinishedCount")
    private long finishedCount = 0;
    @JsonProperty("FailedCount")
    private long failedCount;

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

    public long getFinishedCount() {
        return finishedCount;
    }

    public void setFinishedCount(long finishedCount) {
        this.finishedCount = finishedCount;
    }

    public long getFailedCount() {
        return failedCount;
    }

    public void setFailedCount(long failedCount) {
        this.failedCount = failedCount;
    }

    @Override
    public String toString() {
        return "GenericFabricNode [name=" + name + ", totalCount=" + totalCount + ", finishedCount=" + finishedCount
                + ", failedCount=" + failedCount + "]";
    }
    
    

}
