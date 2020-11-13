package com.latticeengines.ulysses.controller;


import java.util.Date;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.ListSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class ListSegmentSummary {

    @JsonProperty("name")
    private String name;

    @JsonProperty("displayName")
    private String displayName;

    @JsonProperty("externalSystem")
    private String externalSystem;

    @JsonProperty("externalSegmentId")
    private String externalSegmentId;

    @JsonProperty("s3DropFolder")
    private String s3DropFolder;

    @JsonProperty("created")
    private Date created;

    @JsonProperty("createdBy")
    private String createdBy;

    @JsonProperty("updated")
    private Date updated;

    @JsonProperty("updatedBby")
    private String updatedBy;

    static ListSegmentSummary fromMetadataSegment(MetadataSegment metadataSegment) {
        ListSegmentSummary summary =  null;
        if (metadataSegment != null) {
            summary = new ListSegmentSummary();
            summary.setName(metadataSegment.getName());
            summary.setDisplayName(metadataSegment.getDisplayName());
            ListSegment listSegment = metadataSegment.getListSegment();
            if (listSegment != null) {
                summary.setExternalSystem(listSegment.getExternalSystem());
                summary.setExternalSegmentId(listSegment.getExternalSegmentId());
                summary.setS3DropFolder(listSegment.getS3DropFolder());
            }
            summary.setCreated(metadataSegment.getCreated());
            summary.setCreatedBy(metadataSegment.getCreatedBy());
            summary.setUpdated(metadataSegment.getUpdated());
            summary.setUpdatedBy(metadataSegment.getUpdatedBy());
        }
        return summary;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getExternalSystem() {
        return externalSystem;
    }

    public void setExternalSystem(String externalSystem) {
        this.externalSystem = externalSystem;
    }

    public String getExternalSegmentId() {
        return externalSegmentId;
    }

    public void setExternalSegmentId(String externalSegmentId) {
        this.externalSegmentId = externalSegmentId;
    }

    public String getS3DropFolder() {
        return s3DropFolder;
    }

    public void setS3DropFolder(String s3DropFolder) {
        this.s3DropFolder = s3DropFolder;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public Date getUpdated() {
        return updated;
    }

    public void setUpdated(Date updated) {
        this.updated = updated;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }
}
