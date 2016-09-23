package com.latticeengines.domain.exposed.scoringapi;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class EnrichResponse {

    public static final String ENRICH_RESPONSE_METADATA = "enrich_response_metadata";

    private Map<String, Object> enrichmentAttributeValues = new HashMap<>();

    @JsonProperty(ENRICH_RESPONSE_METADATA)
    private EnrichResponseMetadata responseMetadata = new EnrichResponseMetadata();

    @JsonAnyGetter
    public Map<String, Object> getEnrichmentAttributeValues() {
        return enrichmentAttributeValues;
    }

    public void setEnrichmentAttributeValues(Map<String, Object> enrichmentAttributeValues) {
        this.enrichmentAttributeValues = enrichmentAttributeValues;
    }

    @JsonIgnore
    public List<Warning> getWarnings() {
        return responseMetadata.getWarnings();
    }

    @JsonIgnore
    public void setWarnings(List<Warning> warnings) {
        responseMetadata.setWarnings(warnings);
    }

    @JsonIgnore
    public String getTimestamp() {
        return responseMetadata.getTimestamp();
    }

    @JsonIgnore
    public void setTimestamp(String timestamp) {
        responseMetadata.setTimestamp(timestamp);
    }

    @JsonIgnore
    public String getRequestId() {
        return responseMetadata.getRequestId();
    }

    @JsonIgnore
    public void setRequestId(String requestId) {
        responseMetadata.setRequestId(requestId);
    }

    public void setResponseMetadata(EnrichResponseMetadata responseMetadata) {
        this.responseMetadata = responseMetadata;
    }
}
