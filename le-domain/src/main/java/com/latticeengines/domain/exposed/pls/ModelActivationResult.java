package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class ModelActivationResult {
    private boolean exists;

    @JsonProperty("Exists")
    public boolean isExists() {
        return exists;
    }

    @JsonProperty("Exists")
    public void setExists(boolean exists) {
        this.exists = exists;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
