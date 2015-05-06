package com.latticeengines.domain.exposed.admin;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OptionalConfigurationField {

    private String node;
    private List<String> options;
    private String defaultOption;

    public OptionalConfigurationField(){ }

    @JsonProperty("Node")
    public String getNode() { return node; }

    @JsonProperty("Node")
    public void setNode(String node) { this.node = node; }

    @JsonProperty("Options")
    public List<String> getOptions() { return options; }

    @JsonProperty("Options")
    public void setOptions(List<String> options) { this.options = options; }

    @JsonProperty("DefaultOption")
    public String getDefaultOption() { return defaultOption; }

    @JsonProperty("DefaultOption")
    public void setDefaultOption(String option) { this.defaultOption = option; }

}
