package com.latticeengines.telepath.tools;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.telepath.relations.BaseRelation;

public class ValidationReport {
    private Set<BaseRelation> unfulfilledRelations = new HashSet<>();

    // collection of loops
    // each node in a loop expressed as a map entity type and Id
    private Set<List<Map<String, String>>> loops = new HashSet<>();

    private Set<Map<String, String>> tombstones = new HashSet<>();

    public Set<BaseRelation> getUnfulfilledRelations() {
        return unfulfilledRelations;
    }

    public void setUnfulfilledRelations(Set<BaseRelation> unfulfilledRelations) {
        this.unfulfilledRelations = unfulfilledRelations;
    }

    public Set<List<Map<String, String>>> getLoops() {
        return loops;
    }

    public void setLoops(Set<List<Map<String, String>>> loops) {
        this.loops = loops;
    }

    public Set<Map<String, String>> getTombstones() {
        return tombstones;
    }

    public void setTombstones(Set<Map<String, String>> tombstones) {
        this.tombstones = tombstones;
    }

    @JsonProperty("isSuccess")
    public boolean isSuccess() {
        return CollectionUtils.isEmpty(unfulfilledRelations) && CollectionUtils.isEmpty(loops)
                && CollectionUtils.isEmpty(tombstones);
    }
}
