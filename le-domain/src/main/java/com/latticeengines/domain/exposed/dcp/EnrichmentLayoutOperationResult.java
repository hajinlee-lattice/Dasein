package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class EnrichmentLayoutOperationResult {

    @JsonProperty("valid")
    boolean valid;

    @JsonProperty("message")
    String message;

    @JsonProperty("layoutId")
    String layoutId;

    private EnrichmentLayoutOperationResult() {
        throw new UnsupportedOperationException();
    }

    public EnrichmentLayoutOperationResult(boolean valid, String message) {
        this.valid = valid;
        this.message = message;
    }

    public EnrichmentLayoutOperationResult(boolean valid, String message, String layoutId) {
        this.valid = valid;
        this.message = message;
        this.layoutId = layoutId;
    }

    public boolean isValid() {
        return valid;
    }

    public boolean getValid() { return valid; }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public String getLayoutId() {
        return layoutId;
    }

    public void setLayoutId(String layoutId) {
        this.layoutId = layoutId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
