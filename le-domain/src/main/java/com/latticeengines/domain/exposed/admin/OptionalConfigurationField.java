package com.latticeengines.domain.exposed.admin;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OptionalConfigurationField {

    private String configurationPath;
    private List<String> options;

    public OptionalConfigurationField(){ }

    @JsonProperty("ConfigurationPath")
    public String getConfigurationPath() { return configurationPath; }

    @JsonProperty("ConfigurationPath")
    public void setConfigurationPath(String path) { this.configurationPath = path; }

    @JsonProperty("Options")
    public List<String> getOptions() { return options; }

    @JsonProperty("Options")
    public void setOptions(List<String> options) { this.options = options; }

}
