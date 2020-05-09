package com.latticeengines.domain.exposed.datacloud.dataflow.source;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn.Calculation;

public class ColumnCurationParameters extends TransformationFlowParameters {
    @JsonProperty("ColumnOperations")
    private Calculation[] columnOperations;

    // Map to store <operation type, related fields>
    private Map<Calculation, List<String>> typeFieldMap;

    // Map to store <fields, value>
    private Map<String, String> fieldValueMap;

    public Calculation[] getColumnOperations() {
        return columnOperations;
    }

    public void setColumnOperations(Calculation[] columnOperations) {
        this.columnOperations = columnOperations;
    }

    public List<String> getFields(Calculation opType) {
        return typeFieldMap.get(opType);
    }

    public List<String> getValues(Calculation opType) {
        List<String> values = new LinkedList<>();

        List<String> fields = getFields(opType);
        if (CollectionUtils.isNotEmpty(fields)) {
            fields.forEach(field -> values.add(fieldValueMap.get(field)));
        }

        return values;
    }

    public Map<Calculation, List<String>> getTypeFieldMap() {
        return typeFieldMap;
    }

    public void setTypeFieldMap(Map<Calculation, List<String>> typeFieldMap) {
        this.typeFieldMap = typeFieldMap;
    }

    public Map<String, String> getFieldValueMap() {
        return fieldValueMap;
    }

    public void setFieldValueMap(Map<String, String> fieldValueMap) {
        this.fieldValueMap = fieldValueMap;
    }
}
