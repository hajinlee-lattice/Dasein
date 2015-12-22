package com.latticeengines.dataflow.exposed.builder.pivot;

import static com.latticeengines.dataflow.exposed.builder.DataFlowBuilder.FieldMetadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

import cascading.tuple.TupleEntry;

public class PivotMapper implements Serializable {

    private static final long serialVersionUID = -3184163691094094584L;
    public static final int DEFAULT_PRIORITY = 0;

    protected final String keyColumn;
    protected final String valueColumn;
    protected final ImmutableSet<String> pivotedKeys;  // the values in the key column can be pivoted
    protected final ImmutableMap<String, String> columnMap;
    protected final ImmutableMap<String, Class<?>> resultColumnClassMap; // class of each result column
    protected final ImmutableMap<String, Integer> priorityMap; // higher priority overwrites lower priority
    protected final ImmutableMap<String, Object> defaultValues;

    private int defaultPriority = DEFAULT_PRIORITY;
    private Class<?> defaultValueClass = String.class;

    public static PivotMapper pivotToClassWithDefaultValue(String keyColumn, String valueColumn, Set<String> pivotedKeys,
                                                           Class<?> valueClass, Object defaultValue) {
        return new PivotMapper(keyColumn, valueColumn, pivotedKeys, valueClass, null, null, null, defaultValue, null);
    }

    public static PivotMapper pivotToClass(String keyColumn, String valueColumn, Set<String> pivotedKeys,
                                           Class<?> valueClass) {
        return new PivotMapper(keyColumn, valueColumn, pivotedKeys, valueClass, null, null, null, null, null);
    }

    public PivotMapper(String keyColumn,
            String valueColumn,
            Set<String> pivotedKeys,
            Class<?> defaultValueClass,
            Map<String, String> columnMap,
            Map<String, Integer> priorityMap,
            Map<String, Class<?>> resultColumnClassMap,
            Object defaultValue,
            Integer defaultPriority) {
        if (pivotedKeys == null || pivotedKeys.isEmpty()) {
            throw new IllegalArgumentException("Pivot keys is an empty set.");
        }
        Map<String, Object> defaultValues = new HashMap<>();
        for (String key: pivotedKeys) {
            defaultValues.put(key, defaultValue);
        }

        if (StringUtils.isEmpty(keyColumn)) {
            throw new IllegalArgumentException("Key column name cannot be empty.");
        }

        if (StringUtils.isEmpty(valueColumn)) {
            throw new IllegalArgumentException("Value column name cannot be empty.");
        }

        this.keyColumn = keyColumn;
        this.valueColumn = valueColumn;
        if (defaultPriority != null) { this.defaultPriority = defaultPriority; }
        if (defaultValueClass != null) { this.defaultValueClass = defaultValueClass; }

        this.pivotedKeys = ImmutableSet.copyOf(pivotedKeys);
        this.columnMap = ImmutableMap.copyOf(constructColumnMap(columnMap));
        this.resultColumnClassMap = ImmutableMap.copyOf(constructResultColumnClassMap(resultColumnClassMap));
        this.priorityMap = ImmutableMap.copyOf(constructPriorityMap(priorityMap));
        this.defaultValues = ImmutableMap.copyOf(constructDefaultValues(defaultValues));

        checkColumnCollision();
    }

    public PivotMapper(String keyColumn,
            String valueColumn,
            Set<String> pivotedKeys,
            Class<?> defaultValueClass,
            Map<String, String> columnMap,
            Map<String, Integer> priorityMap,
            Map<String, Class<?>> resultColumnClassMap,
            Map<String, Object> defaultValues,
            Integer defaultPriority) {
        if (StringUtils.isEmpty(keyColumn)) {
            throw new IllegalArgumentException("Key column name cannot be empty.");
        }

        if (StringUtils.isEmpty(valueColumn)) {
            throw new IllegalArgumentException("Value column name cannot be empty.");
        }

        if (pivotedKeys == null || pivotedKeys.isEmpty()) {
            throw new IllegalArgumentException("Pivot keys is an empty set.");
        }

        this.keyColumn = keyColumn;
        this.valueColumn = valueColumn;
        if (defaultPriority != null) { this.defaultPriority = defaultPriority; }
        if (defaultValueClass != null) { this.defaultValueClass = defaultValueClass; }

        this.pivotedKeys = ImmutableSet.copyOf(pivotedKeys);
        this.columnMap = ImmutableMap.copyOf(constructColumnMap(columnMap));
        this.resultColumnClassMap = ImmutableMap.copyOf(constructResultColumnClassMap(resultColumnClassMap));
        this.priorityMap = ImmutableMap.copyOf(constructPriorityMap(priorityMap));
        this.defaultValues = ImmutableMap.copyOf(constructDefaultValues(defaultValues));

        checkColumnCollision();
    }

    // map from original key
    private Map<String, String> constructColumnMap(Map<String, String> columnMap) {
        Map<String, String> toReturn = new HashMap<>();
        if (columnMap == null) { columnMap = new HashMap<>(); }
        for (String key: pivotedKeys) {
            String column = columnMap.get(key);
            if (StringUtils.isEmpty(column)) {
                toReturn.put(key, String.valueOf(key)); // nature column naming
            } else {
                toReturn.put(key, column);
            }
        }
        return toReturn;
    }

    // map from original key
    private Map<String, Integer> constructPriorityMap(Map<String, Integer> priorityMap) {
        Map<String, Integer> toReturn = new HashMap<>();
        if (priorityMap == null) { priorityMap = new HashMap<>(); }
        for (String key: pivotedKeys) {
            if (priorityMap.containsKey(key)) {
                toReturn.put(key, priorityMap.get(key));
            } else {
                toReturn.put(key, defaultPriority); // default priority
            }
        }
        return toReturn;
    }

    // map from result columns
    private Map<String, Class<?>> constructResultColumnClassMap(Map<String, Class<?>> classMap) {
        Map<String, Class<?>> toReturn = new HashMap<>();
        if (classMap == null) { classMap = new HashMap<>(); }
        for (String resultColumn: new HashSet<>(columnMap.values())) {
            if (classMap.containsKey(resultColumn)) {
                toReturn.put(resultColumn, classMap.get(resultColumn));
            } else {
                toReturn.put(resultColumn, defaultValueClass);
            }
        }
        return toReturn;
    }

    // map from result columns
    private Map<String, Object> constructDefaultValues(Map<String, Object> defaultValues) {
        Map<String, Object> toReturn = new HashMap<>();
        if (defaultValues == null) { defaultValues = new HashMap<>(); }
        for (String key: resultColumnClassMap.keySet()) {
            if (defaultValues.containsKey(key) && defaultValues.get(key) != null) {
                toReturn.put(key, defaultValues.get(key));
            } else {
                toReturn.put(key, null);    // default value
            }
        }
        return toReturn;
    }

    private void checkColumnCollision() {
        List<ImmutableList<String>> columnPriorityList = new ArrayList<>();
        for (Map.Entry<String, String> entry: columnMap.entrySet()) {
            String column = entry.getValue();
            String key = entry.getKey();
            int priority = priorityMap.get(key);
            columnPriorityList.add(ImmutableList.copyOf(Arrays.asList(column, String.valueOf(priority))));
        }
        Set<ImmutableList<String>> columnPrioritySet = new HashSet<>(columnPriorityList);
        if (columnPrioritySet.size() < columnPriorityList.size()) {
            String columns = StringUtils.join(columnPriorityList, ",");
            throw new IllegalArgumentException("There are column collision in column map: [" + columns + "]");
        }
    }

    public Map<String, Object> getDefaultValues() { return defaultValues; }

    public List<FieldMetadata> getFieldMetadataList() {
        List<FieldMetadata> fieldMetadataList = new ArrayList<>();
        Set<String> columnsVisited = new HashSet<>();
        for (String column: new HashSet<>(columnMap.values())) {
            if (!columnsVisited.contains(column)) {
                Class<?> clazz = resultColumnClassMap.get(column);
                FieldMetadata metadata = new FieldMetadata(column, clazz);
                fieldMetadataList.add(metadata);
                columnsVisited.add(column);
            }
        }
        Collections.sort(fieldMetadataList, new Comparator<FieldMetadata>() {
            @Override
            public int compare(FieldMetadata o1, FieldMetadata o2) {
                return o1.getFieldName().compareTo(o2.getFieldName());
            }
        });
        return fieldMetadataList;
    }

    public Set<String> getResultColumnsLowerCase() {
        Set<String> columns = new HashSet<>();
        for (String key: pivotedKeys) {
            columns.add(columnMap.get(key).toLowerCase());
        }
        return columns;
    }

    public PivotResult pivot(TupleEntry arguments) {
        String key = arguments.getString(keyColumn);
        if (StringUtils.isEmpty(key) || !columnMap.containsKey(key)) { return null; }

        PivotResult result = pivot(key);
        Object value = arguments.getObject(valueColumn);
        result.setValue(castValue(key, value));

        return result;
    }

    public PivotResult pivot(String key) {
        if (columnMap.containsKey(key)) {
            return new PivotResult(columnMap.get(key), priorityMap.get(key));
        } else {
            return null;
        }
    }

    protected Object castValue(String key, Object value) {
        String column = columnMap.get(key);
        Class<?> clazz = resultColumnClassMap.get(column);
        try {
            if (value == null) {
                if (defaultValues.containsKey(column)) {
                    return defaultValues.get(column);
                } else {
                    return null;
                }
            } else if (clazz.isInstance(value)) {
                return value;
            } else {
                String valueInString = String.valueOf(value);
                if (String.class.equals(clazz)) {
                    return valueInString;
                } else if (Integer.class.equals(clazz)) {
                    return Integer.valueOf(valueInString);
                } else if (Long.class.equals(clazz)) {
                    return Long.valueOf(valueInString);
                } else if (Boolean.class.equals(clazz)) {
                    return Boolean.valueOf(valueInString);
                } else {
                    throw new UnsupportedOperationException("Do not know how to cast class " + clazz.getSimpleName());
                }
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_26015, new String[]{ value != null ? value.toString() : "null",
                    value != null ? value.getClass().getSimpleName() : "null", clazz.getSimpleName() });
        }
    }

}
