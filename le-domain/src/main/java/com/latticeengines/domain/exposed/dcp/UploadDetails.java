package com.latticeengines.domain.exposed.dcp;

import javax.persistence.EnumType;
import javax.persistence.Enumerated;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class UploadDetails {

    @JsonProperty("upload_id")
    private String uploadId;

    @JsonProperty("source_id")
    private String sourceId;

    @JsonProperty("status")
    @Enumerated(EnumType.STRING)
    private Upload.Status status;

    @JsonProperty("match_result")
    private String matchResultTableName;

    @JsonProperty("match_candidates")
    private String matchCandidatesTableName;

    @JsonProperty("upload_config")
    private UploadConfig uploadConfig;

    @JsonProperty("upload_stats")
    public UploadStats statistics;

    public String getUploadId() {
        return uploadId;
    }

    public void setUploadId(String uploadId) {
        this.uploadId = uploadId;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public Upload.Status getStatus() {
        return status;
    }

    public void setStatus(Upload.Status status) {
        this.status = status;
    }

    public String getMatchResultTableName() {
        return matchResultTableName;
    }

    public void setMatchResultTableName(String matchResultTableName) {
        this.matchResultTableName = matchResultTableName;
    }

    public String getMatchCandidatesTableName() {
        return matchCandidatesTableName;
    }

    public void setMatchCandidatesTableName(String matchCandidatesTableName) {
        this.matchCandidatesTableName = matchCandidatesTableName;
    }

    public UploadConfig getUploadConfig() {
        return uploadConfig;
    }

    public void setUploadConfig(UploadConfig uploadConfig) {
        this.uploadConfig = uploadConfig;
    }

    public UploadStats getStatistics() {
        return statistics;
    }

    public void setStatistics(UploadStats statistics) {
        this.statistics = statistics;
    }
}
