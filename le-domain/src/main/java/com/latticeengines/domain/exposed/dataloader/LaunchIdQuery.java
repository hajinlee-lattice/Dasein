package com.latticeengines.domain.exposed.dataloader;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LaunchIdQuery {

    @JsonProperty("launch_id")
    private long launchId;

    public long getLaunchId() {
        return launchId;
    }

    public void setLaunchId(long launchId) {
        this.launchId = launchId;
    }
}
