package com.latticeengines.propdata.collection.dataflow.pivot;

import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.strategy.PivotStrategy;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotStrategyImpl;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotType;
import com.latticeengines.domain.exposed.propdata.collection.SourceColumn;

@Component("pivotBaseSource")
public class PivotFlow extends TypesafeDataFlowBuilder<PivotDataFlowParameters> {

    protected static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Node construct(PivotDataFlowParameters parameters) {
        Map<String, Node> sourceMap = new HashMap<>();
        for (String baseTable: parameters.getBaseTables()) {
            sourceMap.put(baseTable, addSource(baseTable));
        }

        List<SourceColumn> columns = parameters.getColumns();
        String[] joinFields = parameters.getJoinFields();

        List<Node> pivotedPipes = pivotPipes(columns, sourceMap, joinFields);
        Node join = joinPipe(joinFields, pivotedPipes.toArray(new Node[pivotedPipes.size()]));

        join = join.addTimestamp("Timestamp");

        return join;
    }


    private Node joinPipe(String[] joinFields, Node[] pipes) {
        FieldList joinFieldList = new FieldList(joinFields);
        Node join = pipes[0];
        for (int i = 1; i < pipes.length; i++) {
            Node rhs = pipes[i];
            join = join.innerJoin(joinFieldList, rhs, joinFieldList);
        }
        return join;
    }

    private List<Node> pivotPipes(List<SourceColumn> columns, Map<String, Node> sourceMap, String[] joinFields) {
        Map<ImmutableList<String>, PivotStrategy> pivotStrategyMap = getPivotStrategyMap(columns);

        List<Node> nodes = new ArrayList<>();

        for (Map.Entry<ImmutableList<String>, PivotStrategy> entry: pivotStrategyMap.entrySet()) {
            ImmutableList<String> keys = entry.getKey();
            String baseSource = keys.get(0);
            String[] groupbyFields = keys.get(1).split(",");
            Node source = sourceMap.get(baseSource);
            Node pivoted = source.pivot(groupbyFields, entry.getValue());
            if (!ImmutableList.copyOf(groupbyFields).equals(ImmutableList.copyOf(joinFields))) {
                pivoted = pivoted.rename(new FieldList(groupbyFields), new FieldList(joinFields));
            }
            pivoted = pivoted.renamePipe("pivoted-" + UUID.randomUUID().toString());
            nodes.add(pivoted);
        }

        return nodes;
    }

    @VisibleForTesting
    static Map<ImmutableList<String>, PivotStrategy> getPivotStrategyMap(List<SourceColumn> columns) {
        Map<SourceColumn, PivotStrategy> strategyMap =  new HashMap<>();
        for (SourceColumn column: columns) {
            strategyMap.put(column, constructPivotStrategy(column));
        }

        Map<ImmutableList<String>, PivotStrategy> pivotStrategyMap =  new HashMap<>();
        for (Map.Entry<SourceColumn, PivotStrategy> entry: strategyMap.entrySet()) {
            SourceColumn column = entry.getKey();
            PivotStrategyImpl impl = (PivotStrategyImpl) entry.getValue();
            ImmutableList<String> identifier = ImmutableList.copyOf(Arrays.asList(
                    column.getBaseSource(), column.getGroupBy(),
                    impl.keyColumn, impl.valueColumn));

            if (pivotStrategyMap.containsKey(identifier)) {
                PivotStrategy oldStrategy = pivotStrategyMap.get(identifier);
                pivotStrategyMap.put(identifier, mergePivotStrategy(oldStrategy, impl));
            } else {
                pivotStrategyMap.put(identifier, impl);
            }
        }
        return pivotStrategyMap;
    }

    private static PivotStrategy constructPivotStrategy(SourceColumn column) {
        PivotType pivotType;
        switch (column.getCalculation()) {
            case PIVOT_ANY:
                pivotType = PivotType.ANY;
                break;
            case PIVOT_MIN:
                pivotType = PivotType.MIN;
                break;
            case PIVOT_MAX:
                pivotType = PivotType.MAX;
                break;
            case PIVOT_SUM:
                pivotType = PivotType.SUM;
                break;
            case PIVOT_COUNT:
                pivotType = PivotType.COUNT;
                break;
            case PIVOT_EXISTS:
                pivotType = PivotType.EXISTS;
                break;
            default:
                pivotType = PivotType.ANY;
        }

        try {
            JsonNode json = objectMapper.readTree(column.getArguments());
            String keyColumn = json.get("PivotKeyColumn").asText();
            String valueColumn = json.get("PivotValueColumn").asText();
            String[] pivotKeys = json.get("TargetPivotKeys").asText().split(",");

            Set<String> keySet = new HashSet<>();
            List<AbstractMap.SimpleImmutableEntry<String, String>> columnMappingList = new ArrayList<>();
            for (String key: pivotKeys) {
                keySet.add(key);
                columnMappingList.add(new AbstractMap.SimpleImmutableEntry<>(key, column.getColumnName()));
            }

            if (column.getColumnType().contains("VARCHAR")) {
                if (json.has("DefaultValue")) {
                    return PivotStrategyImpl.withColumnMap(keyColumn, valueColumn, keySet, columnMappingList,
                            String.class, pivotType, json.get("DefaultValue").asText());
                } else {
                    return PivotStrategyImpl.withColumnMap(keyColumn, valueColumn, keySet, columnMappingList,
                            String.class, pivotType, null);
                }
            } else if (column.getColumnType().contains("INT")) {
                if (json.has("DefaultValue")) {
                    return PivotStrategyImpl.withColumnMap(keyColumn, valueColumn, keySet, columnMappingList,
                            Integer.class, pivotType, json.get("DefaultValue").asInt());
                } else {
                    return PivotStrategyImpl.withColumnMap(keyColumn, valueColumn, keySet, columnMappingList,
                            Integer.class, pivotType, null);
                }
            } else if (column.getColumnType().contains("BIGINT")) {
                if (json.has("DefaultValue")) {
                    return PivotStrategyImpl.withColumnMap(keyColumn, valueColumn, keySet, columnMappingList,
                            Long.class, pivotType, json.get("DefaultValue").asLong());
                } else {
                    return PivotStrategyImpl.withColumnMap(keyColumn, valueColumn, keySet, columnMappingList,
                            Long.class, pivotType, null);
                }
            } else if (column.getColumnType().contains("BIT")) {
                if (json.has("DefaultValue")) {
                    return PivotStrategyImpl.withColumnMap(keyColumn, valueColumn, keySet, columnMappingList,
                            Boolean.class, pivotType, json.get("DefaultValue").asBoolean());
                } else {
                    return PivotStrategyImpl.withColumnMap(keyColumn, valueColumn, keySet, columnMappingList,
                            Boolean.class, pivotType, null);
                }
            } else if (column.getColumnType().contains("DATETIME")) {
                if (json.has("DefaultValue")) {
                    return PivotStrategyImpl.withColumnMap(keyColumn, valueColumn, keySet, columnMappingList,
                            Long.class, pivotType, json.get("DefaultValue").asLong());
                } else {
                    return PivotStrategyImpl.withColumnMap(keyColumn, valueColumn, keySet, columnMappingList,
                            Long.class, pivotType, null);
                }
            } else {
                throw new UnsupportedOperationException("Unknown column type " + column.getColumnType());
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private static PivotStrategy mergePivotStrategy(PivotStrategy pivot1, PivotStrategy pivot2) {
        PivotStrategyImpl impl1 = (PivotStrategyImpl) pivot1;
        PivotStrategyImpl impl2 = (PivotStrategyImpl) pivot2;

        String keyColumn = impl1.keyColumn;
        String valueColumn = impl1.valueColumn;
        List<AbstractMap.SimpleImmutableEntry<String, String>> columnMap = new ArrayList<>(impl1.columnEntryList);
        Map<String, Serializable> defaultValues = new HashMap<>(impl1.defaultValues);
        Set<String> pivotedKeys = new HashSet<>(impl1.pivotedKeys);
        Map<String, Class<?>> classMap = new HashMap<>(impl1.resultColumnClassMap);
        Map<String, PivotType> typeMap =  new HashMap<>(impl1.pivotTypeMap);

        columnMap.addAll(impl2.columnEntryList);
        defaultValues.putAll(impl2.defaultValues);
        pivotedKeys.addAll(impl2.pivotedKeys);
        classMap.putAll(impl2.resultColumnClassMap);
        typeMap.putAll(impl2.pivotTypeMap);

        return new PivotStrategyImpl(keyColumn, valueColumn, pivotedKeys, columnMap,
                classMap, null, typeMap, null, defaultValues, null);
    }
}
