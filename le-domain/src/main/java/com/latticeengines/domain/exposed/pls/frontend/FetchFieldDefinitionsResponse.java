package com.latticeengines.domain.exposed.pls.frontend;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FetchFieldDefinitionsResponse {
    private static final Logger log = LoggerFactory.getLogger(FetchFieldDefinitionsResponse.class);

    @JsonProperty
    protected FieldDefinitionsRecord currentFieldDefinitionsRecord;

    @JsonProperty
    protected ImportWorkflowSpec importWorkflowSpec;

    // Maps from fieldName to FieldDefinition.
    @JsonProperty
    protected Map<String, FieldDefinition> existingFieldDefinitionsMap;

    // Maps from columnName to FieldDefinition.
    // fieldType should be set based on autodetection results.  For DATE types, dateFormat, timeFormat, and timeZone
    // may also be set if successfully autodetected.
    @JsonProperty
    protected Map<String, FieldDefinition> autodetectionResultsMap;

    // Maps from fieldName to OtherTemplateData.  OtherTemplateData contains metadata about the fieldType of this
    // field in other templates of the same Object type and whether this field exists in the batch store.
    @JsonProperty
    protected Map<String, OtherTemplateData> otherTemplateDataMap;

    public FetchFieldDefinitionsResponse() {
        currentFieldDefinitionsRecord = new FieldDefinitionsRecord();
        importWorkflowSpec = new ImportWorkflowSpec();
        existingFieldDefinitionsMap = new HashMap<>();
        autodetectionResultsMap = new HashMap<>();
        otherTemplateDataMap = new HashMap<>();
    }

    public FieldDefinitionsRecord getCurrentFieldDefinitionsRecord() {
        return currentFieldDefinitionsRecord;
    }

    public void setCurrentFieldDefinitionsRecord(FieldDefinitionsRecord currentFieldDefinitionsRecord) {
        this.currentFieldDefinitionsRecord = currentFieldDefinitionsRecord;
    }

    public ImportWorkflowSpec getImportWorkflowSpec() {
        return importWorkflowSpec;
    }

    public void setImportWorkflowSpec(ImportWorkflowSpec importWorkflowSpec) {
        this.importWorkflowSpec = importWorkflowSpec;
    }

    public Map<String, FieldDefinition> getExistingFieldDefinitionsMap() {
        return existingFieldDefinitionsMap;
    }

    public void setExistingFieldDefinitionsMap(Map<String, FieldDefinition> existingFieldDefinitionsMap) {
        this.existingFieldDefinitionsMap = existingFieldDefinitionsMap;
    }

    public Map<String, FieldDefinition> getAutodetectionResultsMap() {
        return autodetectionResultsMap;
    }

    public void setAutodetectionResultsMap(Map<String, FieldDefinition> autodetectionResultsMap) {
        this.autodetectionResultsMap = autodetectionResultsMap;
    }

    public Map<String, OtherTemplateData> getOtherTemplateDataMap() {
        return otherTemplateDataMap;
    }

    public void setOtherTemplateDataMap(Map<String, OtherTemplateData> otherTemplateDataMap) {
        this.otherTemplateDataMap = otherTemplateDataMap;
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof FetchFieldDefinitionsResponse) {
            FetchFieldDefinitionsResponse response = (FetchFieldDefinitionsResponse) object;

            // Check currentFieldDefinitionsRecord.
            if (getCurrentFieldDefinitionsRecord() == null || response.getCurrentFieldDefinitionsRecord() == null) {
                if (getCurrentFieldDefinitionsRecord() != response.getCurrentFieldDefinitionsRecord()) {
                    return false;
                }
            } else if (!getCurrentFieldDefinitionsRecord().equals(response.getCurrentFieldDefinitionsRecord())) {
                return false;
            }

            // Check importWorkflowSpec.
            if (getImportWorkflowSpec() == null || response.getImportWorkflowSpec() == null) {
                if (getImportWorkflowSpec() != response.getImportWorkflowSpec()) {
                    return false;
                }
            } else if (!getImportWorkflowSpec().equals(response.getImportWorkflowSpec())) {
                return false;
            }

            // Check existingFieldDefinitionsMap.
            if (!mapEquals(getExistingFieldDefinitionsMap(), response.getExistingFieldDefinitionsMap())) {
                return false;
            }

            // Check autodetectionResultsMap.
            if (!mapEquals(getAutodetectionResultsMap(), response.getAutodetectionResultsMap())) {
                return false;
            }

            // Check otherTemplateDataMap.
            if (!mapEquals(getOtherTemplateDataMap(), response.getOtherTemplateDataMap())) {
                return false;
            }
            return true;
        }
        return false;
    }

    public static <V> boolean mapEquals(Map<String, V> map1, Map<String, V> map2) {
        if (map1 == null || map2 == null) {
            return map1 == map2;
        } else if (map1.size() != map2.size()) {
            return false;
        }
        for (Map.Entry<String, V> entry : map1.entrySet()) {
            if (!map2.containsKey(entry.getKey())) {
                return false;
            }
            V val1 = entry.getValue();
            V val2 = map2.get(entry.getKey());
            if (val1 == null || val2 == null) {
                if (val1 != val2) {
                    return false;
                }
            } else if (!val1.equals(val2)) {
                return false;
            }
        }
        return true;
    }
}
