package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;

public class ImportExportS3StepConfiguration extends MicroserviceStepConfiguration {

    // For import
    private DataCollection.Version version;

    private CustomEventModelingType customEventModelingType;

    // for export
    private String metadataSegmentExportId;

    public DataCollection.Version getVersion() {
        return version;
    }

    public void setVersion(DataCollection.Version version) {
        this.version = version;
    }

    public String getMetadataSegmentExportId() {
        return metadataSegmentExportId;
    }

    public void setMetadataSegmentExportId(String metadataSegmentExportId) {
        this.metadataSegmentExportId = metadataSegmentExportId;
    }

    public CustomEventModelingType getCustomEventModelingType() {
        return customEventModelingType;
    }

    public void setCustomEventModelingType(CustomEventModelingType customEventModelingType) {
        this.customEventModelingType = customEventModelingType;
    }
}
