package com.latticeengines.domain.exposed.cdl.sla;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ActionConfigPredicate implements Serializable {

    private static final long serialVersionUID = 0L;

    @JsonProperty("json_property")
    private String jsonProperty;

    @JsonProperty("expected_value")
    private List<String> expectedValues;

    public String getJsonProperty() {
        return jsonProperty;
    }

    public void setJsonProperty(String jsonProperty) {
        this.jsonProperty = jsonProperty;
    }

    public List<String> getExpectedValues() {
        return expectedValues;
    }

    public void setExpectedValues(List<String> expectedValues) {
        this.expectedValues = expectedValues;
    }
}
