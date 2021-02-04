package com.latticeengines.domain.exposed.dcp;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class UploadConfig {

    @JsonProperty("dropFilePath")
    private String dropFilePath;

    @JsonProperty("uploadTsPrefix")
    private String uploadTimestamp;

    @JsonProperty("uploadRawFilePath")
    private String uploadRawFilePath;

    @JsonProperty("uploadImportedFilePath")
    private String uploadImportedFilePath;

    @JsonProperty("uploadMatchResultPrefix")
    private String uploadMatchResultPrefix;

    @JsonProperty("uploadImportedErrorFilePath")
    private String uploadImportedErrorFilePath;

    @JsonProperty("sourceOnHdfs")
    private Boolean sourceOnHdfs;

    @JsonProperty("usageReportFilePath")
    private String usageReportFilePath;

    @JsonProperty("suppressKnownMatchErrors")
    private Boolean suppressKnownMatchErrors;

    @JsonProperty("downloadableFiles")
    private Set<DownloadFileType> downloadableFiles;

    public String getDropFilePath() {
        return dropFilePath;
    }

    public void setDropFilePath(String dropFilePath) {
        this.dropFilePath = dropFilePath;
    }

    public String getUploadTimestamp() {
        return uploadTimestamp;
    }

    public void setUploadTimestamp(String uploadTimestamp) {
        this.uploadTimestamp = uploadTimestamp;
    }

    public String getUploadRawFilePath() {
        return uploadRawFilePath;
    }

    public void setUploadRawFilePath(String uploadRawFilePath) {
        this.uploadRawFilePath = uploadRawFilePath;
    }

    public String getUploadImportedFilePath() {
        return uploadImportedFilePath;
    }

    public void setUploadImportedFilePath(String uploadImportedFilePath) {
        this.uploadImportedFilePath = uploadImportedFilePath;
    }

    public String getUploadMatchResultPrefix() {
        return uploadMatchResultPrefix;
    }

    public void setUploadMatchResultPrefix(String uploadMatchResultPrefix) {
        this.uploadMatchResultPrefix = uploadMatchResultPrefix;
    }

    public String getUploadMatchResultAccepted() {
        return uploadMatchResultPrefix + "accepted.csv";
    }

    public String getUploadMatchResultRejected() {
        return uploadMatchResultPrefix + "rejected.csv";
    }

    public String getUploadMatchResultErrored() {
        return uploadMatchResultPrefix + "processing_errors.csv";
    }

    public String getUploadImportedErrorFilePath() {
        return uploadImportedErrorFilePath;
    }

    public void setUploadImportedErrorFilePath(String uploadImportedErrorFilePath) {
        this.uploadImportedErrorFilePath = uploadImportedErrorFilePath;
    }

    public Boolean getSourceOnHdfs() {
        return sourceOnHdfs;
    }

    public void setSourceOnHdfs(Boolean sourceOnHdfs) {
        this.sourceOnHdfs = sourceOnHdfs;
    }

    public String getUsageReportFilePath() {
        return usageReportFilePath;
    }

    public void setUsageReportFilePath(String usageReportFilePath) {
        this.usageReportFilePath = usageReportFilePath;
    }

    public Boolean getSuppressKnownMatchErrors() {
        return suppressKnownMatchErrors;
    }

    public void setSuppressKnownMatchErrors(Boolean suppressKnownMatchErrors) {
        this.suppressKnownMatchErrors = suppressKnownMatchErrors;
    }

    public Set<DownloadFileType> getDownloadableFiles() {
        return downloadableFiles;
    }

    public void setDownloadableFiles(Set<DownloadFileType> downloadableFiles) {
        this.downloadableFiles = downloadableFiles;
    }

    public List<String> getDownloadPaths() {
        return Arrays.asList(uploadRawFilePath, uploadImportedFilePath,
                uploadImportedErrorFilePath, uploadMatchResultPrefix);
    }
}
