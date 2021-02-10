package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AcxiomReceived extends EventDetail {

    public AcxiomReceived() {
        super("Acxiom");
    }

    @JsonProperty("received_count")
    private Long receivedCount;

    public Long getReceivedCount() {
        return receivedCount;
    }

    public void setReceivedCount(Long receivedCount) {
        this.receivedCount = receivedCount;
    }


}
