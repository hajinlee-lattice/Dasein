package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsType;

public class ExportOrphansToS3StepConfiguration extends ImportExportS3StepConfiguration {

    @JsonProperty
    private String exportId;

    @JsonProperty
    private String sourcePath;

    @JsonProperty
    private String dataCollectionName;

    @JsonProperty
    private OrphanRecordsType orphanRecordsType;

    public String getExportId() {
        return exportId;
    }

    public void setExportId(String exportId) {
        this.exportId = exportId;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public void setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
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

    public void setOrphanRecordsType(OrphanRecordsType orphanRecordsType) {
        this.orphanRecordsType = orphanRecordsType;
    }
}
