package com.latticeengines.propdata.dataflow.pivot;

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

import com.latticeengines.dataflow.exposed.builder.common.JoinType;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.strategy.PivotStrategy;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotStrategyImpl;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotType;
import com.latticeengines.domain.exposed.dataflow.BooleanType;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.propdata.dataflow.PivotDataFlowParameters;
import com.latticeengines.domain.exposed.propdata.manage.SourceColumn;
import com.latticeengines.propdata.dataflow.common.FlowUtils;

@Component("pivotFlow")
public class PivotFlow extends TypesafeDataFlowBuilder<PivotDataFlowParameters> {

    protected static ObjectMapper objectMapper = new ObjectMapper();
    private String rowIdField = "RowId" + UUID.randomUUID().toString().replace("-", "");

    @Override
    public Node construct(PivotDataFlowParameters parameters) {
        Map<String, Node> sourceMap = new HashMap<>();
        for (String baseTable : parameters.getBaseTables()) {
            sourceMap.put(baseTable, addSource(baseTable));
        }
        Node join = joinedConfigurablePipes(parameters, sourceMap);
        join = FlowUtils.removeInvalidDatetime(join, parameters.getColumns());
        join = join.addTimestamp(parameters.getTimestampField());
        join = finalRetain(join, parameters.getColumns());
        if (parameters.hasSqlPresence()) {
            join = FlowUtils.truncateStringFields(join, parameters.getColumns());
        }
        return join;
    }

    protected Node finalRetain(Node node, List<SourceColumn> columns) {
        List<String> fields = new ArrayList<>();
        for (SourceColumn column : columns) {
            fields.add(column.getColumnName());
        }
        return node.retain(new FieldList(fields.toArray(new String[fields.size()])));
    }

    protected Node joinedConfigurablePipes(PivotDataFlowParameters parameters, Map<String, Node> sourceMap) {
        Node pivot = joinedPivotedPipes(parameters, sourceMap);
        Node agg = joinedAggregatedPipes(parameters, sourceMap);
        if (pivot != null && agg != null) {
            FieldList joinFieldList = new FieldList(parameters.getJoinFields());
            Node join = pivot.join(joinFieldList, agg, joinFieldList, JoinType.OUTER);
            return join.renamePipe("join");
        } else if (pivot != null) {
            return pivot;
        } else if (agg != null) {
            return agg;
        } else {
            return null;
        }
    }

    private Node joinedPivotedPipes(PivotDataFlowParameters parameters, Map<String, Node> sourceMap) {
        List<SourceColumn> columns = parameters.getColumns();
        String[] joinFields = parameters.getJoinFields();
        List<Node> pivotedPipes = pivotPipes(columns, sourceMap, joinFields);
        if (pivotedPipes.isEmpty()) {
            return null;
        }

        Node join = joinPipe(joinFields, pivotedPipes.toArray(new Node[pivotedPipes.size()]));
        join = join.renamePipe("pivot-joined");
        join = rewriteIsNull(join, parameters.getColumns());
        return join;
    }

    private Node joinedAggregatedPipes(PivotDataFlowParameters parameters, Map<String, Node> sourceMap) {
        List<SourceColumn> columns = parameters.getColumns();
        String[] joinFields = parameters.getJoinFields();
        List<Node> aggPipes = aggregatedPipes(columns, sourceMap, joinFields);
        if (aggPipes.isEmpty()) {
            return null;
        }

        Node join = joinPipe(joinFields, aggPipes.toArray(new Node[aggPipes.size()]));
        join = join.renamePipe("agg-joined");
        join = rewriteIsNull(join, parameters.getColumns());
        return join;
    }

    private Node rewriteIsNull(Node join, List<SourceColumn> columns) {
        Map<String, String> isNullMap = new HashMap<>();
        for (SourceColumn column : columns) {
            try {
                if (StringUtils.isNotEmpty(column.getArguments())) {
                    JsonNode json = objectMapper.readTree(column.getArguments());
                    if (json.has("IsNull")) {
                        String nullReplacement = json.get("IsNull").asText();
                        isNullMap.put(column.getColumnName(), nullReplacement);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (isNullMap.isEmpty()) {
            return join;
        }

        List<FieldMetadata> fms = join.getSchema();
        for (Map.Entry<String, String> entry : isNullMap.entrySet()) {
            String field = entry.getKey();
            String nullValue = entry.getValue();
            FieldMetadata fieldMetadata = null;
            for (FieldMetadata fm : fms) {
                if (fm.getFieldName().equals(field)) {
                    fieldMetadata = fm;
                    break;
                }
            }
            if (fieldMetadata != null) {
                String newValue;
                if (fieldMetadata.getJavaType().equals(String.class)) {
                    newValue = String.format("\"%s\"", nullValue);
                } else {
                    newValue = String.format("new %s(%s)", fieldMetadata.getJavaType().getSimpleName(), nullValue);
                }
                join = join.addFunction(String.format("%s == null ? %s : %s", field, newValue, field), new FieldList(
                        field), fieldMetadata);
            }
        }
        return join;
    }

    private Node joinPipe(String[] joinFields, Node[] pipes) {
        FieldList joinFieldList = new FieldList(joinFields);
        Node join = pipes[0];
        for (int i = 1; i < pipes.length; i++) {
            Node rhs = pipes[i];
            join = join.join(joinFieldList, rhs, joinFieldList, JoinType.OUTER);
        }
        return join;
    }

    private List<Node> aggregatedPipes(List<SourceColumn> columns, Map<String, Node> sourceMap, String[] joinFields) {
        Map<ImmutableList<String>, List<Aggregation>> aggregationMap = getAggregationMap(columns);
        List<Node> nodes = new ArrayList<>();
        for (Map.Entry<ImmutableList<String>, List<Aggregation>> entry : aggregationMap.entrySet()) {
            ImmutableList<String> keys = entry.getKey();
            String baseSource = keys.get(0);
            String[] groupbyFields = keys.get(1).split(",");
            Node source = sourceMap.get(baseSource);
            Node agg = source.groupBy(new FieldList(groupbyFields), entry.getValue());
            if (!ImmutableList.copyOf(groupbyFields).equals(ImmutableList.copyOf(joinFields))) {
                agg = agg.rename(new FieldList(groupbyFields), new FieldList(joinFields));
            }
            agg = agg.renamePipe("agg-" + UUID.randomUUID().toString());
            nodes.add(agg);
        }
        return nodes;
    }

    private List<Node> pivotPipes(List<SourceColumn> columns, Map<String, Node> sourceMap, String[] joinFields) {
        Map<ImmutableList<String>, PivotStrategy> pivotStrategyMap = getPivotStrategyMap(columns);

        List<Node> nodes = new ArrayList<>();

        for (Map.Entry<ImmutableList<String>, PivotStrategy> entry : pivotStrategyMap.entrySet()) {
            ImmutableList<String> keys = entry.getKey();
            String baseSource = keys.get(0);
            String[] groupbyFields = keys.get(1).split(",");
            Node source = sourceMap.get(baseSource);
            if (hasRowCount((PivotStrategyImpl) entry.getValue())) {
                source = source.addRowID(rowIdField);
            }
            Node pivoted = source.pivot(groupbyFields, entry.getValue());
            if (!ImmutableList.copyOf(groupbyFields).equals(ImmutableList.copyOf(joinFields))) {
                pivoted = pivoted.rename(new FieldList(groupbyFields), new FieldList(joinFields));
            }
            pivoted = pivoted.renamePipe("pivoted-" + UUID.randomUUID().toString());
            pivoted = rewriteBoolean(pivoted, columns);
            nodes.add(pivoted);
        }

        return nodes;
    }

    private Node rewriteBoolean(Node pipe, List<SourceColumn> columns) {
        Map<String, BooleanType> bTypeMap = new HashMap<>();
        for (SourceColumn column : columns) {
            try {
                if (StringUtils.isNotEmpty(column.getArguments())) {
                    JsonNode json = objectMapper.readTree(column.getArguments());
                    if (json.has("BooleanType")) {
                        BooleanType bType = BooleanType.valueOf(json.get("BooleanType").asText());
                        bTypeMap.put(column.getColumnName(), bType);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (bTypeMap.isEmpty()) {
            return pipe;
        }

        List<FieldMetadata> fms = pipe.getSchema();
        for (Map.Entry<String, BooleanType> entry : bTypeMap.entrySet()) {
            String field = entry.getKey();
            BooleanType bType = entry.getValue();
            FieldMetadata fieldMetadata = null;
            for (FieldMetadata fm : fms) {
                if (fm.getFieldName().equals(field)) {
                    fieldMetadata = fm;
                    break;
                }
            }
            if (fieldMetadata != null) {
                pipe = pipe.renameBooleanField(field, bType);
            }
        }
        return pipe;
    }

    @VisibleForTesting
    static Map<ImmutableList<String>, List<Aggregation>> getAggregationMap(List<SourceColumn> columns) {
        Map<SourceColumn, Aggregation> tempMap = new HashMap<>();
        for (SourceColumn column : columns) {
            Aggregation aggregation = constructAggregation(column);
            if (aggregation != null) {
                tempMap.put(column, aggregation);
            }
        }

        Map<ImmutableList<String>, List<Aggregation>> aggregationMap = new HashMap<>();
        for (Map.Entry<SourceColumn, Aggregation> entry : tempMap.entrySet()) {
            SourceColumn column = entry.getKey();
            Aggregation aggregation = entry.getValue();
            ImmutableList<String> identifier;
            if (StringUtils.isEmpty(column.getPreparation())) {
                identifier = ImmutableList.copyOf(Arrays.asList(column.getBaseSource(), column.getGroupBy()));
            } else {
                identifier = ImmutableList.copyOf(Arrays.asList(column.getBaseSource() + "_" + column.getPreparation(),
                        column.getGroupBy()));
            }
            if (!aggregationMap.containsKey(identifier)) {
                aggregationMap.put(identifier, new ArrayList<Aggregation>());
            }
            aggregationMap.get(identifier).add(aggregation);
        }
        return aggregationMap;
    }

    private static Aggregation constructAggregation(SourceColumn column) {
        AggregationType aggType;
        switch (column.getCalculation()) {
        case AGG_MIN:
            aggType = AggregationType.MIN;
            break;
        case AGG_MAX:
            aggType = AggregationType.MAX;
            break;
        case AGG_SUM:
            aggType = AggregationType.SUM;
            break;
        case AGG_COUNT:
            aggType = AggregationType.COUNT;
            break;
        default:
            return null;
        }
        try {
            JsonNode json = objectMapper.readTree(column.getArguments());
            String aggColumn = json.get("AggregateColumn").asText();
            return new Aggregation(aggColumn, column.getColumnName(), aggType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @VisibleForTesting
    Map<ImmutableList<String>, PivotStrategy> getPivotStrategyMap(List<SourceColumn> columns) {
        Map<SourceColumn, PivotStrategy> strategyMap = new HashMap<>();
        for (SourceColumn column : columns) {
            PivotStrategy pivotStrategy = constructPivotStrategy(column);
            if (pivotStrategy != null) {
                strategyMap.put(column, pivotStrategy);
            }
        }

        Map<ImmutableList<String>, PivotStrategy> pivotStrategyMap = new HashMap<>();
        for (Map.Entry<SourceColumn, PivotStrategy> entry : strategyMap.entrySet()) {
            SourceColumn column = entry.getKey();
            PivotStrategyImpl impl = (PivotStrategyImpl) entry.getValue();

            ImmutableList<String> identifier;
            if (StringUtils.isEmpty(column.getPreparation())) {
                identifier = ImmutableList.copyOf(Arrays.asList(column.getBaseSource(), column.getGroupBy(),
                        impl.keyColumn, impl.valueColumn));
            } else {
                identifier = ImmutableList.copyOf(Arrays.asList(column.getBaseSource() + "_" + column.getPreparation(),
                        column.getGroupBy(), impl.keyColumn, impl.valueColumn));
            }

            if (pivotStrategyMap.containsKey(identifier)) {
                PivotStrategy oldStrategy = pivotStrategyMap.get(identifier);
                pivotStrategyMap.put(identifier, mergePivotStrategy(oldStrategy, impl));
            } else {
                pivotStrategyMap.put(identifier, impl);
            }
        }
        return pivotStrategyMap;
    }

    private PivotStrategy constructPivotStrategy(SourceColumn column) {
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
            return null;
        }

        try {
            JsonNode json = objectMapper.readTree(column.getArguments());
            String keyColumn = json.get("PivotKeyColumn").asText();
            String valueColumn = json.get("PivotValueColumn").asText();

            if (PivotType.COUNT.equals(pivotType) && "*".equals(valueColumn)) {
                valueColumn = rowIdField;
            }

            String[] pivotKeys = json.get("TargetPivotKeys").asText().split(",");
            Set<String> keySet = new HashSet<>();
            List<AbstractMap.SimpleImmutableEntry<String, String>> columnMappingList = new ArrayList<>();
            for (String key : pivotKeys) {
                keySet.add(key);
                columnMappingList.add(new AbstractMap.SimpleImmutableEntry<>(key, column.getColumnName()));
            }
            String columnType = column.getColumnType();
            if (PivotType.EXISTS.equals(pivotType)) {
                columnType = "BIT";
            }

            if (columnType.contains("VARCHAR")) {
                if (json.has("DefaultValue")) {
                    return PivotStrategyImpl.withColumnMap(keyColumn, valueColumn, keySet, columnMappingList,
                            String.class, pivotType, json.get("DefaultValue").asText());
                } else {
                    return PivotStrategyImpl.withColumnMap(keyColumn, valueColumn, keySet, columnMappingList,
                            String.class, pivotType, null);
                }
            } else if (columnType.contains("INT")) {
                if (json.has("DefaultValue")) {
                    return PivotStrategyImpl.withColumnMap(keyColumn, valueColumn, keySet, columnMappingList,
                            Integer.class, pivotType, json.get("DefaultValue").asInt());
                } else {
                    return PivotStrategyImpl.withColumnMap(keyColumn, valueColumn, keySet, columnMappingList,
                            Integer.class, pivotType, null);
                }
            } else if (columnType.contains("BIGINT")) {
                if (json.has("DefaultValue")) {
                    return PivotStrategyImpl.withColumnMap(keyColumn, valueColumn, keySet, columnMappingList,
                            Long.class, pivotType, json.get("DefaultValue").asLong());
                } else {
                    return PivotStrategyImpl.withColumnMap(keyColumn, valueColumn, keySet, columnMappingList,
                            Long.class, pivotType, null);
                }
            } else if (columnType.contains("BIT")) {
                if (json.has("DefaultValue")) {
                    Boolean defaultValue = Arrays.asList("1", "true", "yes", "t").contains(
                            json.get("DefaultValue").asText().toLowerCase());
                    return PivotStrategyImpl.withColumnMap(keyColumn, valueColumn, keySet, columnMappingList,
                            Boolean.class, pivotType, defaultValue);
                } else {
                    return PivotStrategyImpl.withColumnMap(keyColumn, valueColumn, keySet, columnMappingList,
                            Boolean.class, pivotType, null);
                }
            } else if (columnType.contains("DATETIME")) {
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
        Map<String, PivotType> typeMap = new HashMap<>(impl1.pivotTypeMap);

        columnMap.addAll(impl2.columnEntryList);
        defaultValues.putAll(impl2.defaultValues);
        pivotedKeys.addAll(impl2.pivotedKeys);
        classMap.putAll(impl2.resultColumnClassMap);
        typeMap.putAll(impl2.pivotTypeMap);

        return new PivotStrategyImpl(keyColumn, valueColumn, pivotedKeys, columnMap, classMap, null, typeMap, null,
                defaultValues, null);
    }

    private boolean hasRowCount(PivotStrategyImpl impl) {
        return rowIdField.equals(impl.valueColumn);
    }

}
