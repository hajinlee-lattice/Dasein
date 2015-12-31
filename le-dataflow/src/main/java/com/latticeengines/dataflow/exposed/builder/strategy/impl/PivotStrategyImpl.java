package com.latticeengines.dataflow.exposed.builder.strategy.impl;

import static com.latticeengines.dataflow.exposed.builder.DataFlowBuilder.FieldMetadata;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
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
    public final ImmutableList<AbstractMap.SimpleImmutableEntry<String, String>> columnEntryList;
    public final ImmutableMap<String, Class<?>> resultColumnClassMap; // class of each result column
    public final Map<String, Serializable> defaultValues;
    public final ImmutableMap<String, PivotType> pivotTypeMap;

    public static <T extends Serializable> PivotStrategyImpl withColumnMap(String keyColumn,
                                                                           String valueColumn,
                                                                           Set<String> pivotedKeys,
                                                                           List<AbstractMap.SimpleImmutableEntry<String, String>> columnEntryList,
                                                                           Class<T> resultClass,
                                                                           PivotType type,
                                                                           T defaultValue) {
        return new PivotStrategyImpl(
                keyColumn, valueColumn, pivotedKeys,
                columnEntryList,
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

    public static <T extends Serializable> PivotStrategyImpl sum(String keyColumn, String valueColumn,
                                          Set<String> pivotedKeys, Class<T> resultClass, T defaultValue) {
        return new PivotStrategyImpl(
                keyColumn, valueColumn, pivotedKeys,
                null,
                null, resultClass,
                null, PivotType.SUM,
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
                             List<AbstractMap.SimpleImmutableEntry<String, String>> columnEntryList,
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
        this.columnEntryList = ImmutableList.copyOf(constructColumnEntryList(columnEntryList));
        this.resultColumnClassMap = ImmutableMap.copyOf(constructResultColumnClassMap(resultColumnClassMap, defaultResultClass));
        this.pivotTypeMap = ImmutableMap.copyOf(constructPivotTypeMap(pivotTypeMap, defaultType));
        this.defaultValues = constructDefaultValues(defaultValues, defaultValue);
    }

    // map from original key
    private List<AbstractMap.SimpleImmutableEntry<String, String>>
    constructColumnEntryList(List<AbstractMap.SimpleImmutableEntry<String, String>> columnEntryList) {
        List<AbstractMap.SimpleImmutableEntry<String, String>> toReturn = new ArrayList<>();
        if (columnEntryList == null) { columnEntryList = new ArrayList<>(); }
        for (String key: pivotedKeys) {
            boolean defined = false;
            for (Map.Entry<String, String> entry: columnEntryList) {
                if (entry.getKey().equalsIgnoreCase(key)) {
                    defined = true;
                    AbstractMap.SimpleImmutableEntry<String, String> colMapping =
                            new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), entry.getValue());
                    toReturn.add(colMapping);
                }
            }

            if (!defined) {
                // nature column naming
                AbstractMap.SimpleImmutableEntry<String, String> colMapping =
                        new AbstractMap.SimpleImmutableEntry<>(key, String.valueOf(key));
                toReturn.add(colMapping);
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
        for (String resultColumn: getResultColumns()) {
            if (classMap.containsKey(resultColumn)) {
                toReturn.put(resultColumn, classMap.get(resultColumn));
            } else {
                toReturn.put(resultColumn, defaultResultClass);
            }
        }
        return toReturn;
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
        for (String column: getResultColumns()) {
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
        for (AbstractMap.SimpleImmutableEntry<String, String> entry: columnEntryList) {
            columns.add(entry.getValue());
        }
        return columns;
    }

    @Override
    public List<PivotResult> pivot(TupleEntry arguments) {
        List<PivotResult> toReturn = new ArrayList<>();
        String key = arguments.getString(keyColumn);
        List<PivotResult> results = pivot(key);
        for (PivotResult result: results) {
            if (PivotType.COUNT.equals(result.getPivotType())) {
                Class<?> clz = resultColumnClassMap.get(result.getColumnName());
                if (clz.equals(Integer.class)) {
                    result.setValue(1);
                } else {
                    result.setValue(1L);
                }
            } else {
                Object value = arguments.getObject(valueColumn);
                result.setValue(castValue(result.getColumnName(), value));
            }
            toReturn.add(result);
        }
        return toReturn;
    }

    public List<PivotResult> pivot(String key) {
        List<PivotResult> results = new ArrayList<>();
        if (pivotedKeys.contains(key)) {
            for (String column: resultColumnsFromKey(key)) {
                results.add(new PivotResult(column, pivotTypeMap.get(column)));
            }
        }
        return results;
    }

    private Set<String> resultColumnsFromKey(String key) {
        Set<String> columns = new HashSet<>();
        for (AbstractMap.SimpleImmutableEntry<String, String> entry: columnEntryList) {
            if (entry.getKey().equalsIgnoreCase(key)) {
                columns.add(entry.getValue());
            }
        }
        return columns;
    }

    protected Object castValue(String column, Object value) {
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
