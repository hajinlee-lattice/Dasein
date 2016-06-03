package com.latticeengines.domain.exposed.scoringapi;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

public class Record {
    public static final String LATTICE_ID = "LATTICE";

    @JsonProperty("recordId")
    @ApiModelProperty(value = "Record ID")
    private String recordId;

    @JsonProperty("idType")
    @ApiModelProperty(value = "Type of record ID")
    private String idType;

    @JsonProperty("modelAttributeValuesMap")
    @ApiModelProperty(value = "Map of model ID and attributeValues")
    Map<String, Map<String, Object>> modelAttributeValuesMap;

    @JsonProperty("performEnrichment")
    @ApiModelProperty(value = "Should perform enrichment or not")
    private boolean performEnrichment;

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

    public Map<String, Map<String, Object>> getModelAttributeValuesMap() {
        return modelAttributeValuesMap;
    }

    public void setModelAttributeValuesMap(Map<String, Map<String, Object>> modelAttributeValuesMap) {
        this.modelAttributeValuesMap = modelAttributeValuesMap;
    }

    public boolean isPerformEnrichment() {
        return performEnrichment;
    }

    public void setPerformEnrichment(boolean performEnrichment) {
        this.performEnrichment = performEnrichment;
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
