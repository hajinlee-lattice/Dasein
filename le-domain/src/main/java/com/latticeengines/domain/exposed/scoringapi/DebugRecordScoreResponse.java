package com.latticeengines.domain.exposed.scoringapi;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DebugRecordScoreResponse extends RecordScoreResponse {

    @JsonProperty("transformedRecordMap")
    private Map<String, Map<String, Object>> transformedRecordMap;

    @JsonProperty("transformedRecordMapTypes")
    private Map<String, Map<String, String>> transformedRecordMapTypes;

    @JsonProperty("matchLogs")
    private List<String> matchLogs;

    @JsonProperty("matchErrorMessages")
    private List<String> matchErrorMessages;

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
        if (!MapUtils.isEmpty(transformedRecordMap)) {
            Map<String, Map<String, String>> transformedRecordTypes = new HashMap<>();
            for (String key : transformedRecordMap.keySet()) {
                Map<String, Object> transformedData = transformedRecordMap.get(key);
                if (transformedData != null) {
                    Map<String, String> typeMap = new HashMap<>();
                    transformedRecordTypes.put(key, typeMap);
                    for (String attr : transformedData.keySet()) {
                        typeMap.put(attr, transformedData.get(attr) == null ? //
                                null : transformedData.get(attr).getClass().getName());
                    }
                }
            }
            setTransformedRecordMapTypes(transformedRecordTypes);
        }
    }

    public Map<String, Map<String, String>> getTransformedRecordMapTypes() {
        return transformedRecordMapTypes;
    }

    public void setTransformedRecordMapTypes(Map<String, Map<String, String>> transformedRecordMapTypes) {
        this.transformedRecordMapTypes = transformedRecordMapTypes;
    }

    public List<String> getMatchLogs() {
        return matchLogs;
    }

    public void setMatchLogs(List<String> matchLogs) {
        this.matchLogs = matchLogs;
    }

    public List<String> getMatchErrorMessages() {
        return matchErrorMessages;
    }

    public void setMatchErrorMessages(List<String> matchErrorMessages) {
        this.matchErrorMessages = matchErrorMessages;
    }

}
