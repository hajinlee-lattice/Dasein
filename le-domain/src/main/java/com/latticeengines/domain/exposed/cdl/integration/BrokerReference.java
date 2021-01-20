package com.latticeengines.domain.exposed.cdl.integration;


import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class BrokerReference {

    @JsonProperty("dataStreamId")
    private String dataStreamId;

    @JsonProperty("enabled")
    private boolean enabled;

    @JsonProperty("sourceId")
    private String sourceId;

    @JsonProperty("selectedFields")
    private Map<String, List<String>> selectedFields;

    @JsonProperty("connectionType")
    private InboundConnectionType connectionType;

    public String getDataStreamId() {
        return dataStreamId;
    }

    public void setDataStreamId(String dataStreamId) {
        this.dataStreamId = dataStreamId;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Map<String, List<String>> getSelectedFields() {
        return selectedFields;
    }

    public void setSelectedFields(Map<String, List<String>> selectedFields) {
        this.selectedFields = selectedFields;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public InboundConnectionType getConnectionType() {
        return connectionType;
    }

    public void setConnectionType(InboundConnectionType connectionType) {
        this.connectionType = connectionType;
    }
}
