package com.latticeengines.perf.domain;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

public class ModelDefinition implements HasName {

    private String name;
    private List<Algorithm> algorithms = new ArrayList<Algorithm>();

    @Override
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("algorithms")
    public List<Algorithm> getAlgorithms() {
        return algorithms;
    }

    @JsonProperty("algorithms")
    public void setAlgorithms(List<Algorithm> algorithms) {
        this.algorithms = algorithms;
    }

}
