package com.latticeengines.domain.exposed.pls;


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
public class LatticeInsightsDownloadConfig extends FileDownloadConfig {

    @JsonProperty("onlySelectedAttrs")
    private boolean onlySelectedAttrs;

    public boolean isOnlySelectedAttrs() {
        return onlySelectedAttrs;
    }

    public void setOnlySelectedAttrs(boolean onlySelectedAttrs) {
        this.onlySelectedAttrs = onlySelectedAttrs;
    }
}
