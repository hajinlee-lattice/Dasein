package com.latticeengines.domain.exposed.pls.frontend;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class FieldMappingDocument {
    @JsonProperty
    private List<FieldMapping> fieldMappings;

    @JsonProperty
    private List<String> ignoredFields;

    @JsonProperty
    private List<String> requiredFields = new ArrayList<>();

    @JsonProperty
    private ExtraFieldMappingInfo extraFieldMappingInfo;

    public List<FieldMapping> getFieldMappings() {
        return this.fieldMappings;
    }

    public void setFieldMappings(List<FieldMapping> fieldMappings) {
        this.fieldMappings = fieldMappings;
    }

    public List<String> getIgnoredFields() {
        return this.ignoredFields;
    }

    public void setIgnoredFields(List<String> ignoredFields) {
        this.ignoredFields = ignoredFields;
    }

    public List<String> getRequiredFields() {
        return requiredFields;
    }

    public void setRequiredFields(List<String> requiredFields) {
        this.requiredFields = requiredFields;
    }

    public ExtraFieldMappingInfo getExtraFieldMappingInfo() {
        return extraFieldMappingInfo;
    }

    public void setExtraFieldMappingInfo(ExtraFieldMappingInfo extraFieldMappingInfo) {
        this.extraFieldMappingInfo = extraFieldMappingInfo;
    }

    @JsonIgnore
    public void dedupFieldMappings() {
        if (CollectionUtils.isEmpty(fieldMappings)) {
            return;
        }
        Set<String> mappingKeys = new HashSet<>();
        Iterator<FieldMapping> iterator = fieldMappings.iterator();
        while(iterator.hasNext()) {
            FieldMapping fieldMapping = iterator.next();
            String key = fieldMapping.toString();
            if (mappingKeys.contains(key)) {
                iterator.remove();
            } else {
                mappingKeys.add(key);
            }
        }
    }

}
