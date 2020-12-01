package com.latticeengines.domain.exposed.cdl.export;

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
public class AtlasExportConfig {

    @JsonProperty("addExportTimestamp")
    private boolean addExportTimestamp;

    public boolean getAddExportTimestamp() {
        return addExportTimestamp;
    }

    public void setAddExportTimestamp(boolean addExportTimestamp) {
        this.addExportTimestamp = addExportTimestamp;
    }
}
