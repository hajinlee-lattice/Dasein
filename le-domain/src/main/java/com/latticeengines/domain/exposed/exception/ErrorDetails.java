package com.latticeengines.domain.exposed.exception;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This is a serializable form of an LedpException
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ErrorDetails {
    public ErrorDetails(LedpCode errorCode, String errorMsg, String stackTrace) {
        this.errorCode = errorCode;
        this.errorMsg = errorMsg;
        this.stackTrace = stackTrace;
    }

    public ErrorDetails() {
    }

    @JsonProperty
    private LedpCode errorCode;

    @JsonProperty
    private String errorMsg;

    @JsonProperty
    private String stackTrace;

    public LedpCode getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(LedpCode errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public String getStackTrace() {
        return stackTrace;
    }

    public void setStackTrace(String stackTrace) {
        this.stackTrace = stackTrace;
    }
}
