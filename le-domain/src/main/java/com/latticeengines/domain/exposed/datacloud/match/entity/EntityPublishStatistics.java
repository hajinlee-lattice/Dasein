package com.latticeengines.domain.exposed.datacloud.match.entity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class EntityPublishStatistics {
    @JsonProperty("Request")
    private EntityPublishRequest request;

    @JsonProperty("SeedCount")
    private int seedCount;

    @JsonProperty("LookupCount")
    private int lookupCount;

    public EntityPublishStatistics() {
    }

    public EntityPublishStatistics(int seedCount, int lookupCount) {
        this.seedCount = seedCount;
        this.lookupCount = lookupCount;
    }

    public EntityPublishRequest getRequest() {
        return request;
    }

    public void setRequest(EntityPublishRequest request) {
        this.request = request;
    }

    public int getSeedCount() {
        return seedCount;
    }

    public void setSeedCount(int seedCount) {
        this.seedCount = seedCount;
    }

    public int getLookupCount() {
        return lookupCount;
    }

    public void setLookupCount(int lookupCount) {
        this.lookupCount = lookupCount;
    }

}
