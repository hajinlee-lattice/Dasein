package com.latticeengines.domain.exposed.pls.frontend;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.SourcefileConfig;

public class FieldMappingDocument {
    @JsonProperty
    private List<FieldMapping> fieldMappings;

    @JsonProperty
    private List<String> ignoredFields;

    @JsonProperty
    private List<String> requiredFields = new ArrayList<>();

    @JsonProperty
    private ExtraFieldMappingInfo extraFieldMappingInfo;

    @JsonProperty
    private SourcefileConfig sourcefileConfig;

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

    public SourcefileConfig getSourcefileConfig() {
        return sourcefileConfig;
    }

    public void setSourcefileConfig(SourcefileConfig sourcefileConfig) {
        this.sourcefileConfig = sourcefileConfig;
    }

    @JsonIgnore
    public void dedupFieldMappings() {
        if (CollectionUtils.isEmpty(fieldMappings)) {
            return;
        }
        Set<String> mappingKeys = new HashSet<>();
        Iterator<FieldMapping> iterator = fieldMappings.iterator();
        while (iterator.hasNext()) {
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
