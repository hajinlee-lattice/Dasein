package com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class RegisterExtractConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("import_job_identifier")
    private String importJobIdentifier;

    public String getImportJobIdentifier() {
        return importJobIdentifier;
    }

    public void setImportJobIdentifier(String importJobIdentifier) {
        this.importJobIdentifier = importJobIdentifier;
    }
}
