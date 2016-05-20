package com.latticeengines.domain.exposed.scoringapi;

import io.swagger.annotations.ApiModelProperty;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Record {
    public static final String LATTICE_ID = "LATTICE";

    @JsonProperty("recordId")
    @ApiModelProperty(value = "Record ID")
    private String recordId;

    @JsonProperty("idType")
    @ApiModelProperty(value = "Type of record ID")
    private String idType;

    @JsonProperty("modelIds")
    @ApiModelProperty(value = "List of model ID")
    private List<String> modelIds;

    @JsonProperty("performEnrichment")
    @ApiModelProperty(value = "Should perform enrichment or not")
    private boolean performEnrichment;

    @JsonProperty("attributeValues")
    @ApiModelProperty(value = "Attribute values")
    private Map<String, Object> attributeValues;

    private String rootOperationId;

    private String requestTimestamp;

    public String getRecordId() {
        return recordId;
    }

    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }

    public String getIdType() {
        return idType;
    }

    public void setIdType(String idType) {
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

    public String getRootOperationId() {
        return rootOperationId;
    }

    public void setRootOperationId(String rootOperationId) {
        this.rootOperationId = rootOperationId;
    }

    public String getRequestTimestamp() {
        return requestTimestamp;
    }

    public void setRequestTimestamp(String requestTimestamp) {
        this.requestTimestamp = requestTimestamp;
    }
}
