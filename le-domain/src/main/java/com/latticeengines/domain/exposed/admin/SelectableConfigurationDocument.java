package com.latticeengines.domain.exposed.admin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void patch(SerializableDocumentDirectory sDir) {
        Map<String, Exception> failedNodes = new HashMap<>();
        for (SelectableConfigurationField field: nodes) {
            try {
                field.patch(sDir);
            } catch (Exception e) {
                failedNodes.put(field.getNode(), e);
            }
        }
        if (!failedNodes.isEmpty()) {
            StringBuilder builder = new StringBuilder("Patching options for the following nodes failed:\t\n ");
            for (Map.Entry<String, Exception> entry: failedNodes.entrySet()) {
                builder.append(String.format("%s: %s\t\n", entry.getKey(), entry.getValue().getMessage()));
            }
            throw new IllegalArgumentException(builder.toString());
        }
    }

}
