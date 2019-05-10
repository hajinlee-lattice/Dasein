package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "type")
@JsonSubTypes({ //
        @JsonSubTypes.Type(value = ProgressEventDetail.class, name = "Progress"),
        @JsonSubTypes.Type(value = ProgressEventDetail.class, name = "Completed"),
        @JsonSubTypes.Type(value = ProgressEventDetail.class, name = "InProgress"),
        @JsonSubTypes.Type(value = AudienceCreationEventDetail.class, name = "AudienceCreation"),
        @JsonSubTypes.Type(value = FailedEventDetail.class, name = "Failed"),
        @JsonSubTypes.Type(value = InitiatedEventDetail.class, name = "Initiated") })
public abstract class EventDetail {

    private String type;

    public EventDetail() {

    }

    public EventDetail(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

}
