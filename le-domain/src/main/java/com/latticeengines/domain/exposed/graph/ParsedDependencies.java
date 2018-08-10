package com.latticeengines.domain.exposed.graph;

import java.util.List;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

public class ParsedDependencies {
    private List<Triple<String, String, String>> addDependencies;
    private List<Triple<String, String, String>> removeDependencies;

    public List<Triple<String, String, String>> getAddDependencies() {
        return addDependencies;
    }

    public void setAddDependencies(List<Triple<String, String, String>> addDependencies) {
        this.addDependencies = addDependencies;
    }

    public List<Triple<String, String, String>> getRemoveDependencies() {
        return removeDependencies;
    }

    public void setRemoveDependencies(List<Triple<String, String, String>> removeDependencies) {
        this.removeDependencies = removeDependencies;
    }

    public static Triple<String, String, String> tuple(String objectId, String objectType, String dependencyType) {
        return new ImmutableTriple<String, String, String>(objectId, objectType, dependencyType);
    }
}
