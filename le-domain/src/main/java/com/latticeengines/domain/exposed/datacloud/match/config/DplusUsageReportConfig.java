package com.latticeengines.domain.exposed.datacloud.match.config;

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
public class DplusUsageReportConfig {

    @JsonProperty("enabled")
    private boolean enabled;

    // Point of Arrival Event Identifier.
    // This is an identifier that allows multiple usage records to be grouped to a single input record.
    // e.g. a match and append operation may have a dozen usage records
    // that would all get tied back to the original input record via the POAEID.
    @JsonProperty("poaeIdField")
    private String poaeIdField;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getPoaeIdField() {
        return poaeIdField;
    }

    public void setPoaeIdField(String poaeIdField) {
        this.poaeIdField = poaeIdField;
    }
}
