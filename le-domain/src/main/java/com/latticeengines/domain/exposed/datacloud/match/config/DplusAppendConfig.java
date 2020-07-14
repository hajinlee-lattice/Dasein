package com.latticeengines.domain.exposed.datacloud.match.config;

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
public class DplusAppendConfig {

    @JsonProperty("elementIds")
    private List<String> elementIds;

    // not honored by match api yet
    @JsonProperty("tradeUp")
    private Boolean tradeUp;

    public List<String> getElementIds() {
        return elementIds;
    }

    public void setElementIds(List<String> elementIds) {
        this.elementIds = elementIds;
    }

    public Boolean getTradeUp() {
        return tradeUp;
    }

    public void setTradeUp(Boolean tradeUp) {
        this.tradeUp = tradeUp;
    }

}
