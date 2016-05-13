package com.latticeengines.scoringapi.exposed;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

public class Record {
    @JsonProperty("recordId")
    @ApiModelProperty(value = "Record ID")
    private String recordId;

    @JsonProperty("idType")
    @ApiModelProperty(value = "Type of record ID")
    private IdType idType;

    @JsonProperty("modelIds")
    @ApiModelProperty(value = "List of model ID")
    private List<String> modelIds;

    @JsonProperty("performEnrichment")
    @ApiModelProperty(value = "Should perform enrichment or not")
    private boolean performEnrichment;

    @JsonProperty("attributeValues")
    @ApiModelProperty(value = "Attribute values")
    private Map<String, Object> attributeValues;

    public String getRecordId() {
        return recordId;
    }

    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }

    public IdType getIdType() {
        return idType;
    }

    public void setIdType(IdType idType) {
        this.idType = idType;
    }

    public List<String> getModelIds() {
        return modelIds;
    }

    public void setModelIds(List<String> modelIds) {
        this.modelIds = modelIds;
    }

    public boolean isPerformEnrichment() {
        return performEnrichment;
    }

    public void setPerformEnrichment(boolean performEnrichment) {
        this.performEnrichment = performEnrichment;
    }

    public Map<String, Object> getAttributeValues() {
        return attributeValues;
    }

    public void setAttributeValues(Map<String, Object> attributeValues) {
        this.attributeValues = attributeValues;
    }

    public static enum IdType {
        LATTICE, //
        SALESFORCE;
    }
}
