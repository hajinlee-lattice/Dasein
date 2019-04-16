package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SourceSeedFileMergeTransformerConfig extends TransformerConfig {

    @JsonProperty("SourceFieldName")
    private String sourceFieldName;

    @JsonProperty("SourcePriorityFieldName")
    private String SourcePriorityFieldName;

    @JsonProperty("SourceFieldValues")
    private List<String> sourceFieldValues;

    @JsonProperty("SourcePriorityFieldValues")
    private List<String> sourcePriorityFieldValues;

    public String getSourceFieldName() {
        return sourceFieldName;
    }

    public void setSourceFieldName(String sourceFieldName) {
        this.sourceFieldName = sourceFieldName;
    }

    public String getSourcePriorityFieldName() {
        return SourcePriorityFieldName;
    }

    public void setSourcePriorityFieldName(String sourcePriorityFieldName) {
        SourcePriorityFieldName = sourcePriorityFieldName;
    }

    public List<String> getSourceFieldValues() {
        return sourceFieldValues;
    }

    public void setSourceFieldValues(List<String> sourceFieldValues) {
        this.sourceFieldValues = sourceFieldValues;
    }

    public List<String> getSourcePriorityFieldValues() {
        return sourcePriorityFieldValues;
    }

    public void setSourcePriorityFieldValues(List<String> sourcePriorityFieldValues) {
        this.sourcePriorityFieldValues = sourcePriorityFieldValues;
    }

}
