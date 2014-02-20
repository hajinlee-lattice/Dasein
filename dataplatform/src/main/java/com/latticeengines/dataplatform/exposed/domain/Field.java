package com.latticeengines.dataplatform.exposed.domain;

import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

public class Field implements HasName {

    private String name;
    private List<String> typeInfo;

    @Override
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("type")
    public List<String> getType() {
        return typeInfo;
    }

    public void setType(List<String> typeInfo) {
        this.typeInfo = typeInfo;
    }

}
