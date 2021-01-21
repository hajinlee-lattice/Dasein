package com.latticeengines.domain.exposed.cdl.integration;

import java.util.List;

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
public class BrokerSetupInfo {

    @JsonProperty("selectedFields")
    private List<String> selectedFields;

    @JsonProperty("connectionType")
    private InboundConnectionType connectionType;

    public List<String> getSelectedFields() {
        return selectedFields;
    }

    public void setSelectedFields(List<String> selectedFields) {
        this.selectedFields = selectedFields;
    }

    public InboundConnectionType getConnectionType() {
        return connectionType;
    }

    public void setConnectionType(InboundConnectionType connectionType) {
        this.connectionType = connectionType;
    }
}
