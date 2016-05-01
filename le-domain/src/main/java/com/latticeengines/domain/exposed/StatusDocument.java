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

    private String status;
    private String error;

    @JsonProperty("status")
    public String getStatus() {
        return status;
    }

    @JsonProperty("status")
    public void setStatus(String status) {
        this.status = status;
    }

    public static StatusDocument online() {
        StatusDocument doc = new StatusDocument();
        doc.setStatus(ONLINE);
        return doc;
    }

    public static StatusDocument up() {
        StatusDocument doc = new StatusDocument();
        doc.setStatus(UP);
        return doc;
    }

    public static StatusDocument down() {
        StatusDocument doc = new StatusDocument();
        doc.setStatus(DOWN);
        return doc;
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
