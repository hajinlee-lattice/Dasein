package com.latticeengines.dataflow.exposed.builder.strategy.impl;

import static com.latticeengines.dataflow.exposed.builder.DataFlowBuilder.FieldMetadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.dataflow.exposed.builder.strategy.PivotStrategy;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

import cascading.tuple.TupleEntry;

public class PivotStrategyImpl implements PivotStrategy {

    private static final long serialVersionUID = -3184163691094094584L;
    private static final Class<?> DEFAULT_RESULT_CLASS = String.class;
    private static final PivotType DEFAULT_PIVOT_TYPE = PivotType.ANY;

    public final String keyColumn;
    public final String valueColumn;
    public final ImmutableSet<String> pivotedKeys;  // the values in the key column can be pivoted
    public final ImmutableMap<String, String> columnMap;
    public final ImmutableMap<String, Class<?>> resultColumnClassMap; // class of each result column
    public final Map<String, Serializable> defaultValues;
    public final ImmutableMap<String, PivotType> pivotTypeMap;


    public static <T extends Serializable> PivotStrategyImpl withColumnMap(String keyColumn,
                                                                              String valueColumn,
                                                                              Set<String> pivotedKeys,
                                                                              Map<String, String> columnMap,
                                                                              Class<T> resultClass,
                                                                              PivotType type,
                                                                              T defaultValue) {
        return new PivotStrategyImpl(
                keyColumn, valueColumn, pivotedKeys,
                columnMap,
                null, resultClass,
                null, type,
                null, defaultValue
        );
    }

    public static <T extends Serializable> PivotStrategyImpl any(String keyColumn,
                                                                 String valueColumn,
                                                                 Set<String> pivotedKeys,
                                                                 Class<T> resultClass,
                                                                 T defaultValue) {
        return new PivotStrategyImpl(
                keyColumn, valueColumn, pivotedKeys,
                null,
                null, resultClass,
                null, PivotType.ANY,
                null, defaultValue
        );
    }

    public static <T extends Serializable> PivotStrategyImpl max(String keyColumn,
                                                                 String valueColumn,
                                                                 Set<String> pivotedKeys,
                                                                 Class<T> resultClass,
                                                                 T defaultValue) {
        return new PivotStrategyImpl(
                keyColumn, valueColumn, pivotedKeys,
                null,
                null, resultClass,
                null, PivotType.MAX,
                null, defaultValue
        );
    }

    public static PivotStrategyImpl count(String keyColumn, String valueColumn,
            Set<String> pivotedKeys) {
        return new PivotStrategyImpl(
                keyColumn, valueColumn, pivotedKeys,
                null,
                null, Integer.class,
                null, PivotType.COUNT,
                null, 0
        );
    }

    public static PivotStrategyImpl exists(String keyColumn, Set<String> pivotedKeys) {
        return new PivotStrategyImpl(
                keyColumn, keyColumn, pivotedKeys,
                null,
                null, Boolean.class,
                null, PivotType.EXISTS,
                null, false
        );
    }

    public PivotStrategyImpl(String keyColumn, String valueColumn, Set<String> pivotedKeys,
                             Map<String, String> columnMap,
                             Map<String, Class<?>> resultColumnClassMap, Class<?> defaultResultClass,
                             Map<String, PivotType> pivotTypeMap, PivotType defaultType,
                             Map<String, Serializable> defaultValues, Serializable defaultValue) {
        if (pivotedKeys == null || pivotedKeys.isEmpty()) {
            throw new IllegalArgumentException("Pivot keys is an empty set.");
        }

        if (StringUtils.isEmpty(keyColumn)) {
            throw new IllegalArgumentException("Key column name cannot be empty.");
        }

        if (StringUtils.isEmpty(valueColumn)) {
            throw new IllegalArgumentException("Value column name cannot be empty.");
        }

        this.keyColumn = keyColumn;
        this.valueColumn = valueColumn;
        this.pivotedKeys = ImmutableSet.copyOf(pivotedKeys);
        this.columnMap = ImmutableMap.copyOf(constructColumnMap(columnMap));
        this.resultColumnClassMap = ImmutableMap.copyOf(constructResultColumnClassMap(resultColumnClassMap, defaultResultClass));
        this.pivotTypeMap = ImmutableMap.copyOf(constructPivotTypeMap(pivotTypeMap, defaultType));
        this.defaultValues = constructDefaultValues(defaultValues, defaultValue);

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


    // map from result columns
    private Map<String, Class<?>> constructResultColumnClassMap(Map<String, Class<?>> classMap,
                                                                Class<?> defaultResultClass) {
        Map<String, Class<?>> toReturn = new HashMap<>();
        if (classMap == null) { classMap = new HashMap<>(); }
        if (defaultResultClass == null) { defaultResultClass = DEFAULT_RESULT_CLASS; }
        for (String resultColumn: new HashSet<>(columnMap.values())) {
            if (classMap.containsKey(resultColumn)) {
                toReturn.put(resultColumn, classMap.get(resultColumn));
            } else {
                toReturn.put(resultColumn, defaultResultClass);
            }
        }
        return toReturn;
    }

    private void checkColumnCollision() {
        Set<String> columnClassSet = new HashSet<>();
        Set<String> columnSet = new HashSet<>();
        for (Map.Entry<String, String> entry: columnMap.entrySet()) {
            String column = entry.getValue();
            String className = resultColumnClassMap.get(column).getSimpleName();
            columnClassSet.add(column + "[" + className + "]");
            columnSet.add(column);
        }

        if (columnSet.size() < columnClassSet.size()) {
            String columns = StringUtils.join(columnClassSet, ",");
            throw new IllegalArgumentException("There are column collision in column map: { " + columns + " }");
        }
    }

    private Map<String, PivotType> constructPivotTypeMap(Map<String, PivotType> pivotTypeMap, PivotType defaultType) {
        Map<String, PivotType> toReturn = new HashMap<>();
        if (pivotTypeMap == null) { pivotTypeMap = new HashMap<>(); }
        if (defaultType == null) { defaultType = DEFAULT_PIVOT_TYPE; }
        for (String key: resultColumnClassMap.keySet()) {
            if (pivotTypeMap.containsKey(key) && pivotTypeMap.get(key) != null) {
                toReturn.put(key, pivotTypeMap.get(key));
            } else {
                toReturn.put(key, defaultType);
            }
        }
        return toReturn;
    }

    // map from result columns
    private Map<String, Serializable> constructDefaultValues(Map<String, Serializable> defaultValues,
                                                             Serializable defaultValue) {
        Map<String, Serializable> toReturn = new HashMap<>();
        if (defaultValues == null) { defaultValues = new HashMap<>(); }
        for (String key: resultColumnClassMap.keySet()) {
            if (defaultValues.containsKey(key) && defaultValues.get(key) != null) {
                toReturn.put(key, defaultValues.get(key));
            } else {
                toReturn.put(key, defaultValue);
            }
        }
        return toReturn;
    }

    @Override
    public Map<String, Object> getDefaultValues() {
        Map<String, Object> toReturn = new HashMap<>();
        for (Map.Entry<String, Serializable> entry: defaultValues.entrySet()) {
            toReturn.put(entry.getKey(), entry.getValue());
        }
        return toReturn;
    }

    @Override
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

    @Override
    public Set<String> getResultColumns() {
        Set<String> columns = new HashSet<>();
        for (String key: pivotedKeys) {
            columns.add(columnMap.get(key));
        }
        return columns;
    }

    @Override
    public PivotResult pivot(TupleEntry arguments) {
        String key = arguments.getString(keyColumn);
        if (StringUtils.isEmpty(key) || !columnMap.containsKey(key)) { return null; }
        PivotResult result = pivot(key);
        if (PivotType.COUNT.equals(result.getPivotType())) {
            Class<?> clz = resultColumnClassMap.get(columnMap.get(key));
            if (clz.equals(Integer.class)) {
                result.setValue(1);
            } else {
                result.setValue(1L);
            }
        } else {
            Object value = arguments.getObject(valueColumn);
            result.setValue(castValue(key, value));
        }
        return result;
    }

    public PivotResult pivot(String key) {
        if (columnMap.containsKey(key)) {
            return new PivotResult(columnMap.get(key), pivotTypeMap.get(columnMap.get(key)));
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
