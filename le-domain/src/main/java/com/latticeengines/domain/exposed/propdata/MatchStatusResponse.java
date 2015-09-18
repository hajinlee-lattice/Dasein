package com.latticeengines.domain.exposed.propdata;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MatchStatusResponse {

    private String status;

    public MatchStatusResponse(){}  // for serialization

    public MatchStatusResponse(MatchCommandStatus status) {
        this();
        setStatus(status.getStatus());
    }

    @JsonProperty("Status")
    public String getStatus() {
        return status;
    }

    @JsonProperty("Status")
    public void setStatus(String status) {
        this.status = status;
    }
}
