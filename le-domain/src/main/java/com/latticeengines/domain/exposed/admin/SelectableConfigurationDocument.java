package com.latticeengines.domain.exposed.admin;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SelectableConfigurationDocument {
    private String component;
    private List<SelectableConfigurationField> nodes;

    public SelectableConfigurationDocument(){ }

    @JsonProperty("Nodes")
    public List<SelectableConfigurationField> getNodes() {
        return nodes;
    }

    @JsonProperty("Nodes")
    public void setNodes(List<SelectableConfigurationField> nodes) {
        this.nodes = nodes;
    }

    @JsonProperty("Component")
    public String getComponent() {
        return component;
    }

    @JsonProperty("Component")
    public void setComponent(String component) {
        this.component = component;
    }
}
