package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TalkingPointAttribute {
    @JsonProperty(value = "name")
    private String name;

    @JsonProperty(value = "category")
    private String category;

    @JsonProperty(value = "value")
    private String value;

    public TalkingPointAttribute() {

    }

    public TalkingPointAttribute(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public TalkingPointAttribute(String name, String value, String category) {
        this.name = name;
        this.value = value;
        this.category = category;
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

    public String getCategory() {
        return this.category;
    }

    public void setCategory(String category) {
        this.category = category;
    }
}
