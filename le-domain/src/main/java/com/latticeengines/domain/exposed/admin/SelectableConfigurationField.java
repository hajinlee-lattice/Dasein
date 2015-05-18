package com.latticeengines.domain.exposed.admin;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SelectableConfigurationField {

    private String node;
    private List<String> options;
    private String defaultOption;

    public SelectableConfigurationField(){ }

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

    public void patch(SerializableDocumentDirectory sDir) {
        boolean matchNode = false;
        Iterator<SerializableDocumentDirectory.Node> iter = sDir.getBreathFirstIterator();
        while (iter.hasNext()) {
            SerializableDocumentDirectory.Node dirNode = iter.next();
            if (dirNode.getNode().equals(this.getNode())) {
                matchNode = true;
                SerializableDocumentDirectory.Metadata metadata = dirNode.getMetadata();

                // do not patch if is dynamic options
                if (metadata != null && metadata.isDynamicOptions() != null && metadata.isDynamicOptions()) {
                    throw new IllegalArgumentException("Cannot patch dynamic options.");
                }

                if (metadata == null || !metadata.getType().equals("options")) {
                    metadata = new SerializableDocumentDirectory.Metadata();
                    metadata.setType("options");
                }
                if (this.getOptions() == null) {
                    metadata.setOptions(new ArrayList<String>());
                } else {
                    metadata.setOptions(this.getOptions());
                }
            }
        }
        if (!matchNode) {
            throw new IllegalArgumentException("Cannot find the node to be patched.");
        }
    }

}
