package com.latticeengines.domain.exposed.dante;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TalkingPointAttribute {
    @JsonProperty(value = "name")
    private String name;

    @JsonProperty(value = "value")
    private String value;

    public TalkingPointAttribute() {

    }

    public TalkingPointAttribute(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
