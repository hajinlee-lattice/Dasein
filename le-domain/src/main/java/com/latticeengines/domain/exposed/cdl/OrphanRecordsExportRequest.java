package com.latticeengines.domain.exposed.cdl;

import javax.persistence.EnumType;
import javax.persistence.Enumerated;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact.Status;

public class OrphanRecordsExportRequest {

    @JsonProperty("exportId")
    private String exportId;

    @JsonProperty("createdBy")
    private String createdBy;

    @JsonProperty("dataCollectionName")
    private String dataCollectionName;

    @JsonProperty(value = "type", required = true)
    @Enumerated(EnumType.STRING)
    private OrphanRecordsType orphanRecordsType;

    @JsonProperty(value = "artifactStatus")
    @Enumerated(EnumType.STRING)
    private Status orphanRecordsArtifactStatus;

    @JsonProperty(value = "artifactVersion")
    @Enumerated(EnumType.STRING)
    private DataCollection.Version version;

    public String getExportId() {
        return exportId;
    }

    public void setExportId(String exportId) {
        this.exportId = exportId;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getDataCollectionName() {
        return dataCollectionName;
    }

    public void setDataCollectionName(String dataCollectionName) {
        this.dataCollectionName = dataCollectionName;
    }

    public OrphanRecordsType getOrphanRecordsType() {
        return orphanRecordsType;
    }

    public void setOrphanRecordsType(OrphanRecordsType type) {
        this.orphanRecordsType = type;
    }

    public Status getOrphanRecordsArtifactStatus() {
        return orphanRecordsArtifactStatus;
    }

    public void setOrphanRecordsArtifactStatus(Status orphanRecordsArtifactStatus) {
        this.orphanRecordsArtifactStatus = orphanRecordsArtifactStatus;
    }

    public DataCollection.Version getArtifactVersion() {
        return version;
    }

    public void setArtifactVersion(DataCollection.Version version) {
        this.version = version;
    }
}
