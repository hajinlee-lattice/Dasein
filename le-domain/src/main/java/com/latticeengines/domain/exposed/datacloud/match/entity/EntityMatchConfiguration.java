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
    private final Integer numStagingShards;
    private final String stagingTableName;

    @JsonCreator
    public EntityMatchConfiguration( //
            @JsonProperty("NumStagingShards") Integer numStagingShards, //
            @JsonProperty("StagingTableName") String stagingTableName) {
        this.numStagingShards = numStagingShards;
        this.stagingTableName = stagingTableName;
    }

    @JsonProperty("NumStagingShards")
    public Integer getNumStagingShards() {
        return numStagingShards;
    }

    @JsonProperty("StagingTableName")
    public String getStagingTableName() {
        return stagingTableName;
    }

    @Override
    public String toString() {
        return "EntityMatchConfiguration{" + "numStagingShards=" + numStagingShards + ", stagingTableName='"
                + stagingTableName + '\'' + '}';
    }

    // TODO refactor and move all configuration here
}
