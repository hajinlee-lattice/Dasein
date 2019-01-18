package com.latticeengines.domain.exposed.pls.frontend;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ExtraFieldMappingInfo {

    @JsonProperty("new_mappings")
    private List<FieldMapping> newMappings = new ArrayList<>();

    @JsonProperty("existing_mappings")
    private List<FieldMapping> existingMappings = new ArrayList<>();

    @JsonProperty("missed_mappings")
    private List<FieldMapping> missedMappings = new ArrayList<>();

    public List<FieldMapping> getNewMappings() {
        return newMappings;
    }

    public void setNewMappings(List<FieldMapping> newMappings) {
        this.newMappings = newMappings;
    }

    public void addNewMappings(FieldMapping newMapping) {
        if (this.newMappings == null) {
            this.newMappings = new ArrayList<>();
        }
        this.newMappings.add(newMapping);
    }

    public List<FieldMapping> getExistingMappings() {
        return existingMappings;
    }

    public void setExistingMappings(List<FieldMapping> existingMappings) {
        this.existingMappings = existingMappings;
    }

    public void addExistingMapping(FieldMapping existingMapping) {
        if (this.existingMappings == null) {
            this.existingMappings = new ArrayList<>();
        }
        this.existingMappings.add(existingMapping);
    }

    public List<FieldMapping> getMissedMappings() {
        return missedMappings;
    }

    public void setMissedMappings(List<FieldMapping> missedMappings) {
        this.missedMappings = missedMappings;
    }

    public void addMissedMapping(FieldMapping missedMapping) {
        if (this.missedMappings == null) {
            this.missedMappings = new ArrayList<>();
        }
        this.missedMappings.add(missedMapping);
    }
}
