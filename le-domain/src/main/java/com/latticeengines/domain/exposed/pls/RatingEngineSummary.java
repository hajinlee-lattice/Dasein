package com.latticeengines.domain.exposed.pls;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RatingEngineSummary {

    public RatingEngineSummary() {
    }

    @JsonProperty("id")
    private String id;

    @JsonProperty("displayName")
    private String displayName;

    @JsonProperty("note")
    private String note;

    @JsonProperty("type")
    private RatingEngineType type;

    @JsonProperty("status")
    private RatingEngineStatus status = RatingEngineStatus.INACTIVE;

    @JsonProperty("segmentDisplayName")
    private String segmentDisplayName;

    @JsonProperty("created")
    private Date created;

    @JsonProperty("updated")
    private Date updated;

    @JsonProperty("lastRefreshedDate")
    private Date lastRefreshedDate;

    @JsonProperty("createdBy")
    private String createdBy;

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return this.displayName;
    }

    public void setCreated(Date time) {
        this.created = time;
    }

    public Date getCreated() {
        return this.created;
    }

    public void setUpdated(Date time) {
        this.updated = time;
    }

    public Date getUpdated() {
        return this.updated;
    }

    public void setNote(String note) {
        this.note = note;
    }

    public String getNote() {
        return this.note;
    }

    public void setType(RatingEngineType type) {
        this.type = type;
    }

    public RatingEngineType getType() {
        return this.type;
    }

    public void setStatus(RatingEngineStatus status) {
        this.status = status;
    }

    public RatingEngineStatus getStatus() {
        return this.status;
    }

    public void setSegmentDisplayName(String segmentDisplayName) {
        this.segmentDisplayName = segmentDisplayName;
    }

    public String getSegmentDisplayName() {
        return this.segmentDisplayName;
    }

    public void setCreatedBy(String user) {
        this.createdBy = user;
    }

    public String getCreatedBy() {
        return this.createdBy;
    }

    public void setLastRefreshedDate(Date date) {
        this.lastRefreshedDate = date;
    }

    public Date getLastRefreshedDate() {
        return this.lastRefreshedDate;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
