package com.latticeengines.domain.exposed.datacloud.match;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class TimelineRequest {

    @JsonProperty("MainEntity")
    @NotBlank(message = "Main Entity cannot be empty")
    private String mainEntity;

    @JsonProperty("EntityId")
    @NotBlank(message = "Main Entity cannot be empty")
    private String entityId;

    @JsonProperty("StartTimeStamp")
    @NotNull(message = "Valid start timestamp needed")
    private Long startTimeStamp;

    @JsonProperty("EndTimeStamp")
    @NotNull(message = "Valid end timestamp needed")
    private Long endTimeStamp;

    @JsonProperty("IndexName")
    private String indexName;

    @JsonProperty("CustomerSpace")
    private String customerSpace;

    public String getMainEntity() {
        return mainEntity;
    }

    public void setMainEntity(String mainEntity) {
        this.mainEntity = mainEntity;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public Long getStartTimeStamp() {
        return startTimeStamp;
    }

    public void setStartTimeStamp(Long startTimeStamp) {
        this.startTimeStamp = startTimeStamp;
    }

    public Long getEndTimeStamp() {
        return endTimeStamp;
    }

    public void setEndTimeStamp(Long endTimeStamp) {
        this.endTimeStamp = endTimeStamp;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(String customerSpace) {
        this.customerSpace = customerSpace;
    }
}
