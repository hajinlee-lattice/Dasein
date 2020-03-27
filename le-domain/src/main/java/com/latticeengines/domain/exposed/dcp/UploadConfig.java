package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class UploadConfig {

    @JsonProperty("drop_file_path")
    private String dropFilePath;

    @JsonProperty("upload_ts_prefix")
    private String uploadTSPrefix;

    @JsonProperty("upload_raw_file_path")
    private String uploadRawFilePath;

    @JsonProperty("upload_imported_file_path")
    private String uploadImportedFilePath;

    @JsonProperty("upload_math_result_prefix")
    private String uploadMatchResultPrefix;

    @JsonProperty("upload_imported_error_file_path")
    private String uploadImportedErrorFilePath;

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
}
