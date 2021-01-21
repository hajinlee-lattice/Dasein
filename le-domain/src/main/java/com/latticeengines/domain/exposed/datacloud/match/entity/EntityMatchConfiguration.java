package com.latticeengines.domain.exposed.datacloud.match.entity;

import java.util.Map;
import java.util.Set;

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
    private final Boolean lazyCopyToStaging;
    // entity -> set of field names to skip in match
    private final Map<String, Set<String>> skippedMatchFields;
    private final Map<String, Map<String, Boolean>> allocationModes;

    @JsonCreator
    public EntityMatchConfiguration( //
            @JsonProperty("NumStagingShards") Integer numStagingShards, //
            @JsonProperty("StagingTableName") String stagingTableName, //
            @JsonProperty("LazyCopyToStaging") Boolean lazyCopyToStaging, //
            @JsonProperty("SkippedMatchFields") Map<String, Set<String>> skippedMatchFields,
            @JsonProperty("AllocationModes") Map<String, Map<String, Boolean>> allocationModes) {
        this.numStagingShards = numStagingShards;
        this.stagingTableName = stagingTableName;
        this.lazyCopyToStaging = lazyCopyToStaging;
        this.skippedMatchFields = skippedMatchFields;
        this.allocationModes = allocationModes;
    }

    @JsonProperty("NumStagingShards")
    public Integer getNumStagingShards() {
        return numStagingShards;
    }

    @JsonProperty("StagingTableName")
    public String getStagingTableName() {
        return stagingTableName;
    }

    @JsonProperty("LazyCopyToStaging")
    public Boolean isLazyCopyToStaging() {
        return lazyCopyToStaging;
    }

    @JsonProperty("SkippedMatchFields")
    public Map<String, Set<String>> getSkippedMatchFields() {
        return skippedMatchFields;
    }

    @JsonProperty("AllocationModes")
    public Map<String, Map<String, Boolean>> getAllocationModes() {
        return allocationModes;
    }

    @Override
    public String toString() {
        return "EntityMatchConfiguration{" + "numStagingShards=" + numStagingShards + ", stagingTableName='"
                + stagingTableName + '\'' + ", lazyCopyToStaging=" + lazyCopyToStaging + ", skippedMatchFields="
                + skippedMatchFields + ", allocationModes=" + allocationModes + '}';
    }

    // TODO refactor and move all configuration here
}
