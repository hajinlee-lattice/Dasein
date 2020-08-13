package com.latticeengines.domain.exposed.datacloud.match.entity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * metadata used in entity match
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class EntityMatchConfiguration {
    private final int numStagingShards;

    @JsonCreator
    public EntityMatchConfiguration(@JsonProperty("NumStagingShards") int numStagingShards) {
        this.numStagingShards = numStagingShards;
    }

    @JsonProperty("NumStagingShards")
    public int getNumStagingShards() {
        return numStagingShards;
    }

    @Override
    public String toString() {
        return "EntityMatchConfiguration{" + "numStagingShards=" + numStagingShards + '}';
    }

    // TODO refactor and move all configuration here
}
