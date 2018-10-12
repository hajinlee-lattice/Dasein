package com.latticeengines.domain.exposed.datacloud.match.patch;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;

import java.util.List;

/**
 * Base response entity for DataCloud Patch Validator
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PatchValidationResponse {
    @JsonProperty("Success")
    private boolean success;

    @JsonProperty("PatchBookType")
    private PatchBook.Type patchBookType;

    @JsonProperty("Mode")
    private PatchMode mode;

    @JsonProperty("DataCloudVersion")
    private String dataCloudVersion;

    @JsonProperty("ValidationErrors")
    private List<PatchBookValidationError> validationErrors;

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public PatchBook.Type getPatchBookType() {
        return patchBookType;
    }

    public void setPatchBookType(PatchBook.Type patchBookType) {
        this.patchBookType = patchBookType;
    }

    public PatchMode getMode() {
        return mode;
    }

    public void setMode(PatchMode mode) {
        this.mode = mode;
    }

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

    public List<PatchBookValidationError> getValidationErrors() {
        return validationErrors;
    }

    public void setValidationErrors(List<PatchBookValidationError> validationErrors) {
        this.validationErrors = validationErrors;
    }
}
