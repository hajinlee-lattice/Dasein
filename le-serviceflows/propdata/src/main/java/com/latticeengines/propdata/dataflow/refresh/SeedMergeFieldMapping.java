package com.latticeengines.propdata.dataflow.refresh;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SeedMergeFieldMapping {
    private String sourceColumn;

    private String mainSourceColumn;

    private String mergedSourceColumn;

    private Boolean isDedup;

    @JsonProperty("SourceColumn")
    public String getSourceColumn() {
        return sourceColumn;
    }

    @JsonProperty("SourceColumn")
    public void setSourceColumn(String sourceColumn) {
        this.sourceColumn = sourceColumn;
    }

    @JsonProperty("MainSourceColumn")
    public String getMainSourceColumn() {
        return mainSourceColumn;
    }

    @JsonProperty("MainSourceColumn")
    public void setMainSourceColumn(String mainSourceColumn) {
        this.mainSourceColumn = mainSourceColumn;
    }

    @JsonProperty("MergedSourceColumn")
    public String getMergedSourceColumn() {
        return mergedSourceColumn;
    }

    @JsonProperty("MergedSourceColumn")
    public void setMergedSourceColumn(String mergedSourceColumn) {
        this.mergedSourceColumn = mergedSourceColumn;
    }

    @JsonProperty("IsDedup")
    public Boolean getIsDedup() {
        return isDedup;
    }

    @JsonProperty("IsDedup")
    public void setIsDedup(Boolean isDedup) {
        this.isDedup = isDedup;
    }

}
