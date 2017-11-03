package com.latticeengines.domain.exposed.scoringapi;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;

import io.swagger.annotations.ApiModelProperty;

public class DebugScoreResponse extends ScoreResponse {

    @JsonProperty("probability")
    @ApiModelProperty(required = true)
    private double probability;

    @JsonProperty("matchedRecord")
    private Map<String, Object> matchedRecord;

    @JsonProperty("transformedRecord")
    private Map<String, Object> transformedRecord;

    @JsonProperty("matchedRecordTypes")
    private Map<String, String> matchedRecordTypes;

    @JsonProperty("transformedRecordTypes")
    private Map<String, String> transformedRecordTypes;

    @JsonProperty("matchLogs")
    private List<String> matchLogs;

    @JsonProperty("matchErrorMessages")
    private List<String> matchErrorMessages;

    @JsonProperty("enrichmentMetadataList")
    private List<LeadEnrichmentAttribute> enrichmentMetadataList;

    @JsonProperty("companyInfo")
    private Map<String, Object> companyInfo;

    public Map<String, Object> getMatchedRecord() {
        return matchedRecord;
    }

    public void setMatchedRecord(Map<String, Object> matchedRecord) {
        this.matchedRecord = matchedRecord;
        if (!MapUtils.isEmpty(matchedRecord)) {
            Map<String, String> matchedRecordTypes = new HashMap<>();
            for (String key : matchedRecord.keySet()) {
                matchedRecordTypes.put(key, matchedRecord.get(key) == null ? //
                        null : matchedRecord.get(key).getClass().getName());
            }
            setMatchedRecordTypes(matchedRecordTypes);
        }
    }

    public Map<String, Object> getTransformedRecord() {
        return transformedRecord;
    }

    public void setTransformedRecord(Map<String, Object> transformedRecord) {
        this.transformedRecord = transformedRecord;
        if (!MapUtils.isEmpty(transformedRecord)) {
            Map<String, String> transformedRecordTypes = new HashMap<>();
            for (String key : transformedRecord.keySet()) {
                transformedRecordTypes.put(key, transformedRecord.get(key) == null ? //
                        null : transformedRecord.get(key).getClass().getName());
            }
            setTransformedRecordTypes(transformedRecordTypes);
        }
    }

    public double getProbability() {
        return probability;
    }

    public void setProbability(double probability) {
        this.probability = probability;
    }

    public Map<String, String> getMatchedRecordTypes() {
        return matchedRecordTypes;
    }

    public void setMatchedRecordTypes(Map<String, String> matchedRecordTypes) {
        this.matchedRecordTypes = matchedRecordTypes;
    }

    public Map<String, String> getTransformedRecordTypes() {
        return transformedRecordTypes;
    }

    public void setTransformedRecordTypes(Map<String, String> transformedRecordTypes) {
        this.transformedRecordTypes = transformedRecordTypes;
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

    public List<LeadEnrichmentAttribute> getEnrichmentMetadataList() {
        return enrichmentMetadataList;
    }

    public void setEnrichmentMetadataList(List<LeadEnrichmentAttribute> enrichmentMetadataList) {
        this.enrichmentMetadataList = enrichmentMetadataList;
    }

    public Map<String, Object> getCompanyInfo() {
        return companyInfo;
    }

    public void setCompanyInfo(Map<String, Object> companyInfo) {
        this.companyInfo = companyInfo;
    }

}
