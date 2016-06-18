package com.latticeengines.scoringapi.exposed;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;

import io.swagger.annotations.ApiModelProperty;

public class DebugRecordScoreResponse extends RecordScoreResponse {

    @JsonProperty("transformedRecordMap")
    @ApiModelProperty(required = true)
    private Map<String, Map<String, Object>> transformedRecordMap;

    public DebugRecordScoreResponse() {
    }

    public DebugRecordScoreResponse(RecordScoreResponse recordResponse) {
        setId(recordResponse.getId());
        setEnrichmentAttributeValues(recordResponse.getEnrichmentAttributeValues());
        setLatticeId(recordResponse.getLatticeId());
        setScores(recordResponse.getScores());
        setTimestamp(recordResponse.getTimestamp());
        setWarnings(recordResponse.getWarnings());
    }

    public Map<String, Map<String, Object>> getTransformedRecordMap() {
        return transformedRecordMap;
    }

    public void setTransformedRecordMap(Map<String, Map<String, Object>> transformedRecordMap) {
        this.transformedRecordMap = transformedRecordMap;
    }

}
