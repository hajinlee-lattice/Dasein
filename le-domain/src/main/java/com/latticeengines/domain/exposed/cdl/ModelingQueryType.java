package com.latticeengines.domain.exposed.cdl;

public enum ModelingQueryType {
    TARGET("Target"), //
    TRAINING("Training"), //
    EVENT("Event");

    private String name;

    ModelingQueryType(String name) {
        this.name = name;
    }

    public String getModelingQueryTypeName() {
        return this.name;
    }

}
