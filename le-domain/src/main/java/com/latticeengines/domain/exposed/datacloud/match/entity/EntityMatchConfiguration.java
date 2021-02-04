package com.latticeengines.domain.exposed.datacloud.match.entity;

import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ObjectUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
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
    private final Boolean allocateId;
    private final Map<String, Map<String, Boolean>> allocationModes;

    @JsonCreator
    public EntityMatchConfiguration( //
            @JsonProperty("NumStagingShards") Integer numStagingShards, //
            @JsonProperty("StagingTableName") String stagingTableName, //
            @JsonProperty("LazyCopyToStaging") Boolean lazyCopyToStaging, //
            @JsonProperty("SkippedMatchFields") Map<String, Set<String>> skippedMatchFields, //
            @JsonProperty("AllocateId") Boolean allocateId, //
            @JsonProperty("AllocationModes") Map<String, Map<String, Boolean>> allocationModes) {
        this.numStagingShards = numStagingShards;
        this.stagingTableName = stagingTableName;
        this.lazyCopyToStaging = lazyCopyToStaging;
        this.skippedMatchFields = skippedMatchFields;
        this.allocateId = allocateId;
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

    @JsonProperty("AllocateId")
    public Boolean getAllocateId() {
        return allocateId;
    }

    @JsonProperty("AllocationModes")
    public Map<String, Map<String, Boolean>> getAllocationModes() {
        return allocationModes;
    }

    @JsonIgnore
    // layer values from current object on top of the other object
    public EntityMatchConfiguration mergeWith(EntityMatchConfiguration that) {
        if (that == null) {
            return this;
        }
        return new EntityMatchConfiguration( //
                ObjectUtils.defaultIfNull(numStagingShards, that.numStagingShards), //
                ObjectUtils.defaultIfNull(stagingTableName, that.stagingTableName), //
                ObjectUtils.defaultIfNull(lazyCopyToStaging, that.lazyCopyToStaging), //
                ObjectUtils.defaultIfNull(skippedMatchFields, that.skippedMatchFields), //
                ObjectUtils.defaultIfNull(allocateId, that.allocateId), //
                ObjectUtils.defaultIfNull(allocationModes, that.allocationModes) //
        );
    }

    @Override
    public String toString() {
        return "EntityMatchConfiguration{" + "numStagingShards=" + numStagingShards + ", stagingTableName='"
                + stagingTableName + '\'' + ", lazyCopyToStaging=" + lazyCopyToStaging + ", skippedMatchFields="
                + skippedMatchFields + ", allocateId=" + allocateId + ", allocationModes=" + allocationModes + '}';
    }

    // TODO refactor and move all configuration here
}
