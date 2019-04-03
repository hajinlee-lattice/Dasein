package com.latticeengines.domain.exposed;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class StatusDocument {

    public static final String ONLINE = "online";
    public static final String DOWN = "DOWN";
    public static final String UP = "UP";
    public static final String OK = "OK";
    public static final String MATCHER_IS_BUSY = "MATCHER_IS_BUSY";
    public static final String UNDER_MAINTENANCE = "UNDER_MAINTENANCE";

    private String status;
    private String message;

    // for JSON constructor
    @SuppressWarnings("unused")
    private StatusDocument() {
    }

    public StatusDocument(String status) {
        this.status = status;
    }

    public StatusDocument(String status, String message) {
        this.status = status;
        this.message = message;
    }

    public static StatusDocument ok() {
        return new StatusDocument(OK);
    }

    public static StatusDocument online() {
        return new StatusDocument(ONLINE);
    }

    public static StatusDocument up() {
        return new StatusDocument(UP);
    }

    public static StatusDocument down() {
        return new StatusDocument(DOWN);
    }

    public static StatusDocument matcherIsBusy() {
        return new StatusDocument(MATCHER_IS_BUSY);
    }

    public static StatusDocument matcherIsBusy(String message) {
        return new StatusDocument(MATCHER_IS_BUSY, message);
    }

    public static StatusDocument underMaintenance(String message) {
        return new StatusDocument(UNDER_MAINTENANCE, message);
    }

    @JsonProperty("status")
    public String getStatus() {
        return status;
    }

    @JsonProperty("status")
    public void setStatus(String status) {
        this.status = status;
    }

    @JsonProperty("message")
    public String getMessage() {
        return message;
    }

    @JsonProperty("message")
    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public boolean equals(Object object) {
        return EqualsBuilder.reflectionEquals(this, object);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }
}
