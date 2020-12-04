package com.latticeengines.domain.exposed.dcp;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.FileDownloadConfig;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class UploadFileDownloadConfig extends FileDownloadConfig {

    public UploadFileDownloadConfig() {
    }

    @JsonProperty("uploadId")
    private String uploadId;

    @JsonProperty
    private Boolean includeRaw;

    @JsonProperty
    private Boolean includeMatched;

    @JsonProperty
    private Boolean includeUnmatched;

    @JsonProperty
    private Boolean includeIngestionErrors;

    @JsonProperty
    private Boolean includeProcessingErrors;

    public String getUploadId() {
        return uploadId;
    }

    public void setUploadId(String uploadId) {
        this.uploadId = uploadId;
    }

    public Boolean getIncludeRaw() {
        return includeRaw;
    }

    public void setIncludeRaw(Boolean includeRaw) {
        this.includeRaw = includeRaw;
    }

    public Boolean getIncludeMatched() {
        return includeMatched;
    }

    public void setIncludeMatched(Boolean includeMatched) {
        this.includeMatched = includeMatched;
    }

    public Boolean getIncludeUnmatched() {
        return includeUnmatched;
    }

    public void setIncludeUnmatched(Boolean includeUnmatched) {
        this.includeUnmatched = includeUnmatched;
    }

    public Boolean getIncludeIngestionErrors() {
        return includeIngestionErrors;
    }

    public void setIncludeIngestionErrors(Boolean includeIngestionErrors) {
        this.includeIngestionErrors = includeIngestionErrors;
    }

    public Boolean getIncludeProcessingErrors() {
        return includeProcessingErrors;
    }

    public void setIncludeProcessingErrors(Boolean includeProcessingErrors) {
        this.includeProcessingErrors = includeProcessingErrors;
    }
}
