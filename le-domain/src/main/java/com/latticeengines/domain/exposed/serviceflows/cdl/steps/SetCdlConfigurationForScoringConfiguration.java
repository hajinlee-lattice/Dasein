package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class SetCdlConfigurationForScoringConfiguration extends MicroserviceStepConfiguration {

    @NotEmptyString
    @NotNull
    @JsonProperty("modelingServiceHdfsBaseDir")
    private String modelingServiceHdfsBaseDir;

    @JsonProperty("inputProperties")
    private Map<String, String> inputProperties = new HashMap<>();

    public Map<String, String> getInputProperties() {
        return inputProperties;
    }

    public void setInputProperties(Map<String, String> inputProperties) {
        this.inputProperties = inputProperties;
    }

    public String getModelingServiceHdfsBaseDir() {
        return modelingServiceHdfsBaseDir;
    }

    public void setModelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
        this.modelingServiceHdfsBaseDir = modelingServiceHdfsBaseDir;
    }

}
