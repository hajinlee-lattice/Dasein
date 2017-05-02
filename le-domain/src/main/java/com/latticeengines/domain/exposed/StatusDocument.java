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
    public static final String UNDER_MAINTAINANCE = "UNDER_MAINTAINANCE";

    private String status;

    // for JSON constructor
    @SuppressWarnings("unused")
    private StatusDocument() {}

    public StatusDocument(String status) {
        this.status = status;
    }

    @JsonProperty("status")
    public String getStatus() {
        return status;
    }

    @JsonProperty("status")
    public void setStatus(String status) {
        this.status = status;
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

    public static StatusDocument underMaintainance() {
        return new StatusDocument(UNDER_MAINTAINANCE);
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
