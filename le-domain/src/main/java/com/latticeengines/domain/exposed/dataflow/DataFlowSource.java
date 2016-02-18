package com.latticeengines.domain.exposed.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;

public class DataFlowSource implements HasName {

    private String name;
    private String rawDataPath;
    private boolean purgeAfterUse;

    @Override
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("raw_data_path")
    public String getRawDataPath() {
        return rawDataPath;
    }

    @JsonProperty("raw_data_path")
    public void setRawDataPath(String rawDataPath) {
        this.rawDataPath = rawDataPath;
    }

    public boolean getPurgeAfterUse() {
        return purgeAfterUse;
    }

    public void setPurgeAfterUse(boolean purgeAfterUse) {
        this.purgeAfterUse = purgeAfterUse;
    }
}