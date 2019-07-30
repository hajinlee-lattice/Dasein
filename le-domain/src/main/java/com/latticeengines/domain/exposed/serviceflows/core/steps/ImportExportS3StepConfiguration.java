package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;

public class ImportExportS3StepConfiguration extends MicroserviceStepConfiguration {

    // For import
    private DataCollection.Version version;

    private CustomEventModelingType customEventModelingType;

    // for export
    private String atlasExportId;

    public DataCollection.Version getVersion() {
        return version;
    }

    public void setVersion(DataCollection.Version version) {
        this.version = version;
    }

    public String getAtlasExportId() {
        return atlasExportId;
    }

    public void setAtlasExportId(String metadataSegmentExportId) {
        this.atlasExportId = metadataSegmentExportId;
    }

    public CustomEventModelingType getCustomEventModelingType() {
        return customEventModelingType;
    }

    public void setCustomEventModelingType(CustomEventModelingType customEventModelingType) {
        this.customEventModelingType = customEventModelingType;
    }
}
