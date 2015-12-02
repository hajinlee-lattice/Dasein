package com.latticeengines.serviceflows.workflow.modeling;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class ModelStepConfiguration extends MicroserviceStepConfiguration {

    @NotEmptyString
    @NotNull
    private String modelingServiceHdfsBaseDir;

    @NotNull
    private List<String> eventColumns = null;

    @JsonProperty("modelingServiceHdfsBaseDir")
    public String getModelingServiceHdfsBaseDir() {
        return modelingServiceHdfsBaseDir;
    }

    @JsonProperty("modelingServiceHdfsBaseDir")
    public void setModelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
        this.modelingServiceHdfsBaseDir = modelingServiceHdfsBaseDir;
    }

    @JsonProperty
    public List<String> getEventColumns() {
        return eventColumns;
    }

    @JsonProperty
    public void setEventColumns(List<String> eventColumns) {
        this.eventColumns = eventColumns;
    }

}
