package com.latticeengines.domain.exposed.api;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StringList {

    private List<String> elements = new ArrayList<String>();

    public StringList() {
    }

    public StringList(List<String> elements) {
        this.elements = elements;
    }

    @JsonProperty("elements")
    public List<String> getElements() {
        return elements;
    }

}
