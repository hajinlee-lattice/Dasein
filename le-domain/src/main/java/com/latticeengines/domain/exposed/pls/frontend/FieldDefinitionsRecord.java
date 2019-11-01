package com.latticeengines.domain.exposed.pls.frontend;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FieldDefinitionsRecord {
    private static final Logger log = LoggerFactory.getLogger(FieldDefinitionsRecord.class);

    @JsonProperty(required = false)
    protected String systemName;

    @JsonProperty(required = false)
    protected String systemType;

    @JsonProperty(required = false)
    protected String systemObject;

    // Contains the current state of the field definitions prior to the application of the requested changes.
    @JsonProperty(required = true)
    protected Map<String, List<FieldDefinition>> fieldDefinitionsRecordsMap;

    public FieldDefinitionsRecord() {
        systemName = null;
        systemType = null;
        systemObject = null;
        fieldDefinitionsRecordsMap = new HashMap<>();
    }

    public FieldDefinitionsRecord(String name, String type, String object) {
        systemName = name;
        systemType = type;
        systemObject = object;
        fieldDefinitionsRecordsMap = new HashMap<>();
    }

    public String getSystemName() {
        return systemName;
    }

    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }

    public String getSystemType() {
        return systemType;
    }

    public void setSystemType(String systemType) {
        this.systemType = systemType;
    }

    public String getSystemObject() {
        return systemObject;
    }

    public void setSystemObject(String systemObject) {
        this.systemObject = systemObject;
    }

    public Map<String, List<FieldDefinition>> getFieldDefinitionsRecordsMap() {
        return fieldDefinitionsRecordsMap;
    }

    public void setFieldDefinitionsRecordsMap(Map<String, List<FieldDefinition>> fieldDefinitionsRecordsMap) {
        this.fieldDefinitionsRecordsMap = fieldDefinitionsRecordsMap;
    }

    // Get the field definitions records for a specific section.
    public List<FieldDefinition> getFieldDefinitionsRecords(String fieldSectionName) {
        if (MapUtils.isNotEmpty(fieldDefinitionsRecordsMap) &&
                fieldDefinitionsRecordsMap.containsKey(fieldSectionName)) {
            return fieldDefinitionsRecordsMap.get(fieldSectionName);
        }
        return null;
    }

    // Add the FieldDefinition records for one field section.  Returns true if provided FieldDefinition list was added
    // to the map.
    public boolean addFieldDefinitionsRecords(String fieldSectionName, List<FieldDefinition> fieldDefinitionList,
                                              boolean replaceExisting) {
        if (fieldDefinitionsRecordsMap == null) {
            fieldDefinitionsRecordsMap = new HashMap<>();
        }
        if (replaceExisting || !fieldDefinitionsRecordsMap.containsKey(fieldSectionName)) {
            fieldDefinitionsRecordsMap.put(fieldSectionName, fieldDefinitionList);
            return true;
        }
        return false;
    }

    // Add single FieldDefinition to one field section.  Returns true if provided FieldDefinition was added to the map.
    // TODO(jwinter): Refine how the replace existing logic works.
    public boolean addFieldDefinition(String fieldSectionName, FieldDefinition fieldDefinition,
                                      boolean replaceExisting) {
        if (fieldDefinition == null) {
            log.error("Tried to add null fieldDefinition to section " + fieldSectionName);
            return false;
        }
        if (fieldDefinitionsRecordsMap == null) {
            fieldDefinitionsRecordsMap = new HashMap<>();
        }
        List<FieldDefinition> fieldDefinitionsList;
        if (!fieldDefinitionsRecordsMap.containsKey(fieldSectionName)) {
            fieldDefinitionsList = new ArrayList<>();
            fieldDefinitionsRecordsMap.put(fieldSectionName, fieldDefinitionsList);
        } else {
            fieldDefinitionsList = fieldDefinitionsRecordsMap.get(fieldSectionName);
        }
        for (int i = 0; i < fieldDefinitionsList.size(); i++) {
            if ((StringUtils.isNotBlank(fieldDefinitionsList.get(i).getFieldName()) &&
                    StringUtils.isNotBlank(fieldDefinition.getFieldName()) &&
                    fieldDefinitionsList.get(i).getFieldName().equals(fieldDefinition.getFieldName())) ||
                    (StringUtils.isNotBlank(fieldDefinitionsList.get(i).getColumnName()) &&
                            StringUtils.isNotBlank(fieldDefinition.getColumnName()) &&
                            fieldDefinitionsList.get(i).getColumnName().equals(fieldDefinition.getColumnName()))) {
                if (replaceExisting) {
                    fieldDefinitionsList.add(i, fieldDefinition);
                    return true;
                }
                return false;
            }
        }
        fieldDefinitionsList.add(fieldDefinition);
        return true;
    }

    public FieldDefinition getFieldDefinition(String fieldSectionName, String fieldName) {
        if (StringUtils.isBlank(fieldSectionName) || StringUtils.isBlank(fieldName)) {
            return null;
        } else if (MapUtils.isEmpty(fieldDefinitionsRecordsMap) ||
                !fieldDefinitionsRecordsMap.containsKey(fieldSectionName)) {
            return null;
        }
        List<FieldDefinition> fieldDefinitionList = fieldDefinitionsRecordsMap.get(fieldSectionName);
        for (FieldDefinition fieldDefinition : fieldDefinitionList) {
            if (fieldDefinition != null && fieldName.equals(fieldDefinition.getFieldName())) {
                return fieldDefinition;
            }
        }
        return null;
    }


    @Override
    public boolean equals(Object object) {
        if (object instanceof FieldDefinitionsRecord) {
            FieldDefinitionsRecord record = (FieldDefinitionsRecord) object;

            if (!StringUtils.equals(this.systemName, record.systemName) ||
                    !StringUtils.equals(this.systemType, record.systemType) ||
                    !StringUtils.equals(this.systemObject, record.systemObject)) {
                return false;
            }

            if (this.getFieldDefinitionsRecordsMap() == null || record.getFieldDefinitionsRecordsMap() == null) {
                return this.getFieldDefinitionsRecordsMap() == record.getFieldDefinitionsRecordsMap();
            } else if (this.getFieldDefinitionsRecordsMap().size() != record.getFieldDefinitionsRecordsMap().size()) {
                return false;
            }
            Map<String, List<FieldDefinition>> map2 = record.getFieldDefinitionsRecordsMap();
            for (Map.Entry<String, List<FieldDefinition>> entry: this.getFieldDefinitionsRecordsMap().entrySet()) {
                if (!map2.containsKey(entry.getKey())) {
                    return false;
                }
                List<FieldDefinition> definitionList1 = entry.getValue();
                List<FieldDefinition> definitionList2 = map2.get(entry.getKey());
                if (definitionList1 == null || definitionList2 == null) {
                    if (definitionList1 != definitionList2) {
                        return false;
                    }
                } else {
                    if (definitionList1.size() != definitionList2.size()) {
                        return false;
                    }
                    for (int i = 0; i < definitionList1.size(); i++) {
                        if (!definitionList1.get(i).equals(definitionList2.get(i))) {
                            return false;
                        }
                    }
                }
            }
            return true;
        }
        return false;
    }

}
