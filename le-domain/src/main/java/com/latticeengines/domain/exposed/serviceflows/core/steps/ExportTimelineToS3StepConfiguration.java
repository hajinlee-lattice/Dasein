package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.DataCollection;

public class ExportTimelineToS3StepConfiguration extends ImportExportS3StepConfiguration {

    @JsonProperty
    private DataCollection.Version version;


}
