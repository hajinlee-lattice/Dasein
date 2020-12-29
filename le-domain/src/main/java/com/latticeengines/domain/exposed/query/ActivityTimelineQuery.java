package com.latticeengines.domain.exposed.query;

import java.time.Instant;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ActivityTimelineQuery {

    @JsonProperty("main_entity")
    @NotBlank(message = "Main Entity cannot be empty")
    private BusinessEntity mainEntity;

    @JsonProperty("entity_id")
    @NotBlank(message = "Main Entity cannot be empty")
    private String entityId;

    @JsonProperty("start_timestamp")
    @NotNull(message = "Valid start timestamp needed")
    private Instant startTimeStamp;

    @JsonProperty("end_timestamp")
    @NotNull(message = "Valid end timestamp needed")
    private Instant endTimeStamp;

    public BusinessEntity getMainEntity() {
        return mainEntity;
    }

    public void setMainEntity(BusinessEntity mainEntity) {
        this.mainEntity = mainEntity;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public Instant getStartTimeStamp() {
        return startTimeStamp;
    }

    public void setStartTimeStamp(Instant startTimeStamp) {
        this.startTimeStamp = startTimeStamp;
    }

    public Instant getEndTimeStamp() {
        return endTimeStamp;
    }

    public void setEndTimeStamp(Instant endTimeStamp) {
        this.endTimeStamp = endTimeStamp;
    }
}
