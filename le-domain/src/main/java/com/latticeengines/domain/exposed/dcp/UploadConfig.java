package com.latticeengines.domain.exposed.dcp;

import java.util.Arrays;
import java.util.List;

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
    private String uploadTSPrefix;

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

    @JsonProperty("dropFileTime")
    private Long dropFileTime;

    public String getDropFilePath() {
        return dropFilePath;
    }

    public void setDropFilePath(String dropFilePath) {
        this.dropFilePath = dropFilePath;
    }

    public String getUploadTSPrefix() {
        return uploadTSPrefix;
    }

    public void setUploadTSPrefix(String uploadTSPrefix) {
        this.uploadTSPrefix = uploadTSPrefix;
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

    public List<String> getDownloadPaths() {
        return Arrays.asList(uploadRawFilePath, uploadImportedFilePath,
                uploadImportedErrorFilePath, uploadMatchResultPrefix);
    }

    public Long getDropFileTime() {
        return dropFileTime;
    }

    public void setDropFileTime(Long dropFileTime) {
        this.dropFileTime = dropFileTime;
    }
}
