package com.latticeengines.domain.exposed.serviceflows.core.steps;

public class ExportToS3StepConfiguration extends MicroserviceStepConfiguration {

    private String metadataSegmentExportId;

    public String getMetadataSegmentExportId() {
        return metadataSegmentExportId;
    }

    public void setMetadataSegmentExportId(String metadataSegmentExportId) {
        this.metadataSegmentExportId = metadataSegmentExportId;
    }
}
