package com.latticeengines.domain.exposed.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ThrottleSubmission {

    private boolean immediate;

    public ThrottleSubmission() {
    }

    public ThrottleSubmission(boolean immediate) {
        this.immediate = immediate;
    }

    @JsonProperty("immediate")
    public boolean isImmediate() {
        return immediate;
    }

    @JsonProperty("immediate")
    public void setImmediate(boolean immediate) {
        this.immediate = immediate;
    }

}
