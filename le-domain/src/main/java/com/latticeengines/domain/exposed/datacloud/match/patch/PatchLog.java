package com.latticeengines.domain.exposed.datacloud.match.patch;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

/**
 * Log entry to describe the patch result regarding some PatchBook entry
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PatchLog {
    @JsonProperty("OriginalValue")
    private Object originalValue;

    @JsonProperty("PatchedValue")
    private Object patchedValue;

    @JsonProperty("PatchBookId")
    private Long patchBookId;

    @JsonProperty("Message")
    private String message;

    @JsonProperty("Status")
    private PatchStatus status;

    @JsonProperty("InputMatchKey")
    private MatchKeyTuple inputMatchKey;

    public Object getOriginalValue() {
        return originalValue;
    }

    public void setOriginalValue(Object originalValue) {
        this.originalValue = originalValue;
    }

    public Object getPatchedValue() {
        return patchedValue;
    }

    public void setPatchedValue(Object patchedValue) {
        this.patchedValue = patchedValue;
    }

    public Long getPatchBookId() {
        return patchBookId;
    }

    public void setPatchBookId(Long patchBookId) {
        this.patchBookId = patchBookId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public PatchStatus getStatus() {
        return status;
    }

    public void setStatus(PatchStatus status) {
        this.status = status;
    }

    public MatchKeyTuple getInputMatchKey() {
        return inputMatchKey;
    }

    public void setInputMatchKey(MatchKeyTuple inputMatchKey) {
        this.inputMatchKey = inputMatchKey;
    }
}
