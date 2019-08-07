package com.latticeengines.dataflow.exposed.builder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.exposed.builder.operations.AddFieldOperation;
import com.latticeengines.dataflow.exposed.builder.operations.AggregationOperation;
import com.latticeengines.dataflow.exposed.builder.operations.BitDecodeOperation;
import com.latticeengines.dataflow.exposed.builder.operations.BitEncodeOperation;
import com.latticeengines.dataflow.exposed.builder.operations.CheckPointOperation;
import com.latticeengines.dataflow.exposed.builder.operations.DepivotOperation;
import com.latticeengines.dataflow.exposed.builder.operations.FunctionOperation;
import com.latticeengines.dataflow.exposed.builder.operations.GroupByAndAggOperation;
import com.latticeengines.dataflow.exposed.builder.operations.GroupByAndBufferOperation;
import com.latticeengines.dataflow.exposed.builder.operations.HashJoinOperation;
import com.latticeengines.dataflow.exposed.builder.operations.InsertOperation;
import com.latticeengines.dataflow.exposed.builder.operations.JoinOperation;
import com.latticeengines.dataflow.exposed.builder.operations.KVOperation;
import com.latticeengines.dataflow.exposed.builder.operations.LimitOperation;
import com.latticeengines.dataflow.exposed.builder.operations.MergeOperation;
import com.latticeengines.dataflow.exposed.builder.operations.Operation;
import com.latticeengines.dataflow.exposed.builder.operations.PivotOperation;
import com.latticeengines.dataflow.exposed.builder.operations.RenameOperation;
import com.latticeengines.dataflow.exposed.builder.operations.RenamePipeOperation;
import com.latticeengines.dataflow.exposed.builder.operations.RetainOperation;
import com.latticeengines.dataflow.exposed.builder.operations.SampleOperation;
import com.latticeengines.dataflow.exposed.builder.operations.SortOperation;
import com.latticeengines.dataflow.exposed.builder.operations.TransformFunctionOperation;
import com.latticeengines.dataflow.exposed.builder.strategy.KVAttrPicker;
import com.latticeengines.dataflow.exposed.builder.strategy.PivotStrategy;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.AddColumnWithFixedValueStrategy;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.AddStringFieldStrategy;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.AddTimestampStrategy;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.AddUUIDStrategy;
import com.latticeengines.dataflow.runtime.cascading.AppendOptLogFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.AddRandomIntFunction;
import com.latticeengines.domain.exposed.dataflow.BooleanType;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.domain.exposed.dataflow.operations.OperationLogUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.Sort;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;

import cascading.operation.Aggregator;
import cascading.operation.Buffer;
import cascading.operation.Function;
import cascading.operation.buffer.FirstNBuffer;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public class Node {
    private String identifier;
    private CascadingDataFlowBuilder builder;

    // explicitly scoped in this manner
    Node(String identifier, CascadingDataFlowBuilder builder) {
        this.identifier = identifier;
        this.builder = builder;
    }

    public Node join(String lhsJoinField, Node rhs, String rhsJoinField, JoinType joinType) {
        return join(new FieldList(lhsJoinField), rhs, new FieldList(rhsJoinField), joinType);
    }

    public Node join(FieldList lhsJoinFields, Node rhs, FieldList rhsJoinFields, JoinType joinType) {
        return join(lhsJoinFields, rhs, rhsJoinFields, joinType, false);
    }

    public Node join(FieldList lhsJoinFields, Node rhs, FieldList rhsJoinFields, JoinType joinType, boolean hashJoin) {
        return new Node(builder.register(new JoinOperation(opInput(identifier), lhsJoinFields, opInput(rhs.identifier),
                rhsJoinFields, joinType, hashJoin)), builder);
    }

    public Node coGroup(FieldList lhsFields, List<Node> groupNodes, List<FieldList> groupFieldLists,
            JoinType joinType) {

        List<String> identifiers = new ArrayList<>();
        List<FieldList> fieldLists = new ArrayList<>();

        identifiers.add(identifier);
        fieldLists.add(lhsFields);
        for (Node node : groupNodes) {
            identifiers.add(node.getIdentifier());
        }
        fieldLists.addAll(groupFieldLists);

        return new Node(builder.addCoGroup(identifiers, fieldLists, joinType), builder);
    }

    public Node hashJoin(FieldList lhsFields, List<Node> joinNodes, List<FieldList> joinFields, JoinType joinType) {
        List<Operation.Input> inputs = new ArrayList<>();
        List<FieldList> joinFieldList = new ArrayList<>();
        inputs.add(opInput(identifier));
        joinFieldList.add(lhsFields);
        joinFieldList.addAll(joinFields);
        for (Node node : joinNodes) {
            inputs.add(opInput(node.identifier));
        }
        return new Node(builder.register(new HashJoinOperation(inputs, joinFieldList, joinType)), builder);
    }

    public Node innerJoin(FieldList lhsJoinFields, Node rhs, FieldList rhsJoinFields) {
        return join(lhsJoinFields, rhs, rhsJoinFields, JoinType.INNER);
    }

    public Node innerJoin(String lhsField, Node rhs, String rhsField) {
        return innerJoin(new FieldList(lhsField), rhs, new FieldList(rhsField));
    }

    public Node innerJoin(FieldList lhsJoinFields, Node rhs, FieldList rhsJoinFields, boolean hashJoin) {
        return join(lhsJoinFields, rhs, rhsJoinFields, JoinType.INNER, hashJoin);
    }

    public Node leftJoin(FieldList lhsJoinFields, Node rhs, FieldList rhsJoinFields) {
        return join(lhsJoinFields, rhs, rhsJoinFields, JoinType.LEFT);
    }

    public Node leftJoin(String lhsField, Node rhs, String rhsField) {
        return leftJoin(new FieldList(lhsField), rhs, new FieldList(rhsField));
    }

    public Node leftHashJoin(FieldList lhsJoinFields, Node rhs, FieldList rhsJoinFields) {
        return join(lhsJoinFields, rhs, rhsJoinFields, JoinType.LEFT, true);
    }

    public Node leftHashJoin(String lhsField, Node rhs, String rhsField) {
        return leftHashJoin(new FieldList(lhsField), rhs, new FieldList(rhsField));
    }

    public Node outerJoin(FieldList lhsJoinFields, Node rhs, FieldList rhsJoinFields) {
        return join(lhsJoinFields, rhs, rhsJoinFields, JoinType.OUTER);
    }

    public Node outerJoin(String lhsField, Node rhs, String rhsField) {
        return outerJoin(new FieldList(lhsField), rhs, new FieldList(rhsField));
    }

    public Node hashJoin(FieldList lhsJoinFields, Node rhs, FieldList rhsJoinFields, JoinType joinType) {
        return new Node(builder.addHashJoin(identifier, lhsJoinFields, rhs.identifier, rhsJoinFields, joinType),
                builder);
    }

    public Node groupBy(FieldList groupByFieldList, List<Aggregation> aggregations) {
        return new Node(builder.addGroupBy(identifier, groupByFieldList, aggregations), builder);
    }

    public Node groupBy(FieldList groupByFieldList, FieldList sortFieldList, List<Aggregation> aggregations) {
        return new Node(builder.addGroupBy(identifier, groupByFieldList, sortFieldList, aggregations), builder);
    }

    // group by and limit is better to use buffer, because it stops iterating
    // once it got first N tuples
    public Node groupByAndLimit(FieldList groupByFieldList, int count) {
        return new Node(
                builder.register(
                        new GroupByAndBufferOperation(opInput(identifier), groupByFieldList, new FirstNBuffer(count))),
                builder);
    }

    // group by and limit is better to use buffer, because it stops iterating
    // once it got first N tuples
    public Node groupByAndLimit(FieldList groupByFieldList, FieldList sortFieldList,
            Map<String, Comparator<?>> comparators, int count) {
        return groupByAndBuffer(groupByFieldList, sortFieldList, comparators, new FirstNBuffer(count));
    }

    // group by and limit is better to use buffer, because it stops iterating
    // once it got first N tuples
    public Node groupByAndLimit(FieldList groupByFieldList, FieldList sortFieldList, int count, boolean descending,
            boolean caseInsensitive) {
        return groupByAndBuffer(groupByFieldList, sortFieldList, new FirstNBuffer(count), descending, caseInsensitive);
    }

    @SuppressWarnings("rawtypes")
    public Node groupByAndBuffer(FieldList groupByFieldList, FieldList sortFieldList, Buffer buffer,
            boolean descending) {
        return groupByAndBuffer(groupByFieldList, sortFieldList, buffer, descending, false);
    }

    @SuppressWarnings("rawtypes")
    public Node groupByAndBuffer(FieldList groupByFieldList, FieldList sortFieldList, Buffer buffer, boolean descending,
            boolean caseInsensitive) {
        return new Node(builder.register(new GroupByAndBufferOperation(opInput(identifier), groupByFieldList,
                sortFieldList, buffer, descending, caseInsensitive)), builder);
    }

    @SuppressWarnings("rawtypes")
    public Node groupByAndBuffer(FieldList groupByFieldList, FieldList sortFieldList,
            Map<String, Comparator<?>> comparators, Buffer buffer) {
        return new Node(builder.register(new GroupByAndBufferOperation(opInput(identifier), groupByFieldList,
                sortFieldList, comparators, buffer)), builder);
    }

    @SuppressWarnings("rawtypes")
    public Node groupByAndBuffer(FieldList groupByFieldList, Buffer buffer) {
        return new Node(builder.register(new GroupByAndBufferOperation(opInput(identifier), groupByFieldList, buffer)),
                builder);
    }

    @SuppressWarnings("rawtypes")
    public Node groupByAndBuffer(FieldList groupByFieldList, Buffer buffer, List<FieldMetadata> fieldMetadatas) {
        return new Node(
                builder.register(
                        new GroupByAndBufferOperation(opInput(identifier), groupByFieldList, buffer, fieldMetadatas)),
                builder);
    }

    /**
     * USE CASE: Group by and buffer. Able to track dataflow operation logs if
     * withOptLog = true and in output there will be an additional field
     * LE_OperationLogs
     *
     * ATTENTION: If withOptLog = true, in Buffer constructor, should call
     * BaseGroupbyBuffer(fieldDeclaration, withOptLog).
     *
     * @param groupByFieldList:
     *            fields to group by
     * @param buffer
     * @param fieldMetadatas:
     *            field metadatas for output
     * @param withOptLog:
     *            whether to append log in LE_OperationLogs field
     *
     * @return
     */
    @SuppressWarnings("rawtypes")
    public Node groupByAndBuffer(FieldList groupByFieldList, Buffer buffer, List<FieldMetadata> fieldMetadatas,
            boolean withOptLog) {
        Preconditions.checkNotNull(groupByFieldList);
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(groupByFieldList.getFieldsAsList()));
        Preconditions.checkNotNull(buffer);

        if (withOptLog) {
            if (this.getSchema(OperationLogUtils.DEFAULT_FIELD_NAME) == null //
                    && !fieldMetadatas.stream()
                            .anyMatch(fm -> OperationLogUtils.DEFAULT_FIELD_NAME.equals(fm.getFieldName()))) {
                fieldMetadatas.add(new FieldMetadata(OperationLogUtils.DEFAULT_FIELD_NAME, String.class));
            }
        }

        return new Node(
                builder.register(
                        new GroupByAndBufferOperation(opInput(identifier), groupByFieldList, buffer, fieldMetadatas)),
                builder);
    }

    @SuppressWarnings("rawtypes")
    public Node groupByAndBuffer(FieldList groupByFieldList, FieldList sortFieldList, Buffer buffer,
            List<FieldMetadata> fieldMetadatas) {
        return new Node(
                builder.register(
                        new GroupByAndBufferOperation(opInput(identifier), groupByFieldList, sortFieldList, buffer, fieldMetadatas)),
                builder);
    }

    @SuppressWarnings("rawtypes")
    public Node groupByAndBuffer(FieldList groupByFieldList, FieldList sortFieldList, Buffer buffer, boolean descending,
                                 List<FieldMetadata> fieldMetadatas) {
        return new Node(builder.register(new GroupByAndBufferOperation(
            opInput(identifier), groupByFieldList, sortFieldList, buffer, descending, fieldMetadatas)), builder);
    }

    @SuppressWarnings("rawtypes")
    public Node groupByAndAggregate(FieldList groupByFieldList, FieldList sortFieldList, Aggregator aggregator,
            boolean descending, boolean caseInsensitive) {
        return new Node(builder.register(new GroupByAndAggOperation(opInput(identifier), groupByFieldList,
                sortFieldList, aggregator, descending, caseInsensitive)), builder);
    }

    @SuppressWarnings("rawtypes")
    public Node groupByAndAggregate(FieldList groupByFieldList, Aggregator aggregator) {
        return new Node(builder.register(new GroupByAndAggOperation(opInput(identifier), groupByFieldList, aggregator)),
                builder);
    }

    @SuppressWarnings("rawtypes")
    public Node groupByAndAggregate(FieldList groupByFieldList, Aggregator aggregator,
            List<FieldMetadata> fieldMetadatas) {
        return new Node(
                builder.register(
                        new GroupByAndAggOperation(opInput(identifier), groupByFieldList, aggregator, fieldMetadatas)),
                builder);
    }


    /**
     * USE CASE: Group by and aggregate. Able to track dataflow operation logs
     * if withOptLog = true and in output there will be an additional field
     * LE_OperationLogs
     *
     * ATTENTION: If withOptLog = true, in Aggregator constructor, should call
     * BaseAggregator(fieldDeclaration, withOptLog).
     *
     * @param groupByFieldList:
     *            fields to group by
     * @param aggregator
     * @param fieldMetadatas:
     *            field metadatas for output
     * @param withOptLog:
     *            whether to append log in LE_OperationLogs field
     * @return
     */
    @SuppressWarnings("rawtypes")
    public Node groupByAndAggregate(@NotNull FieldList groupByFieldList, @NotNull Aggregator aggregator,
            @NotNull List<FieldMetadata> fieldMetadatas, boolean withOptLog) {
        Preconditions.checkNotNull(groupByFieldList);
        Preconditions.checkNotNull(aggregator);
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(fieldMetadatas));

        if (withOptLog) {
            if (this.getSchema(OperationLogUtils.DEFAULT_FIELD_NAME) == null //
                    && !fieldMetadatas.stream()
                            .anyMatch(fm -> OperationLogUtils.DEFAULT_FIELD_NAME.equals(fm.getFieldName()))) {
                fieldMetadatas.add(new FieldMetadata(OperationLogUtils.DEFAULT_FIELD_NAME, String.class));
            }
        }

        return new Node(
                builder.register(
                        new GroupByAndAggOperation(opInput(identifier), groupByFieldList, aggregator, fieldMetadatas)),
                builder);
    }

    @SuppressWarnings("rawtypes")
    public Node groupByAndAggregate(FieldList groupByFieldList, Aggregator aggregator,
            List<FieldMetadata> fieldMetadatas, Fields fieldSelectStrategy) {
        return new Node(builder.register(new GroupByAndAggOperation(opInput(identifier), groupByFieldList, aggregator,
                fieldMetadatas, fieldSelectStrategy)), builder);
    }

    public Node groupByAndExpand(FieldList groupByFieldList, String expandField, List<String> expandFormats, //
            FieldList argumentsFieldList, FieldList declaredFieldList) {
        return new Node(builder.addGroupByAndExpand(identifier, groupByFieldList, expandField, //
                expandFormats, argumentsFieldList, declaredFieldList), builder);
    }

    public Node sort(String field) {
        return new Node(builder.register(new SortOperation(opInput(identifier), field)), builder);
    }

    public Node sort(String field, boolean descending) {
        return new Node(builder.register(new SortOperation(opInput(identifier), field, descending)), builder);
    }

    public Node sort(List<String> fields, boolean descending) {
        return new Node(builder.register(new SortOperation(opInput(identifier), fields, descending)), builder);
    }

    public Node sort(Sort sort) {
        return new Node(builder.register(new SortOperation(opInput(identifier), sort)), builder);
    }

    public Node filter(String expression, FieldList filterFieldList) {
        Node filtered = new Node(builder.addFilter(identifier, expression, filterFieldList), builder);
        return filtered.retain(new FieldList(this.getFieldNames()));
    }

    public Node pivot(String[] groupyByFields, PivotStrategy pivotStrategy) {
        return new Node(builder.register(new PivotOperation(opInput(identifier), groupyByFields, pivotStrategy)),
                builder);
    }

    public Node pivot(String[] groupyByFields, PivotStrategy pivotStrategy, boolean caseInsensitiveGroupBy) {
        return new Node(builder.register(new PivotOperation(opInput(identifier), groupyByFields, pivotStrategy, caseInsensitiveGroupBy)),
                builder);
    }

    public Node depivot(String[] targetFields, String[][] sourceFieldTuples) {
        DepivotOperation operation = new DepivotOperation(opInput(identifier), targetFields, sourceFieldTuples);
        return new Node(builder.register(operation), builder);
    }

    public Node renameBooleanField(String booleanField, BooleanType type) {
        String expression;
        FieldMetadata fm;
        switch (type) {
        case TRUE_FALSE:
            expression = String.format("%s ? \"True\" : \"False\"", booleanField);
            fm = new FieldMetadata(booleanField, String.class);
            break;
        case YES_NO:
            expression = String.format("%s ? \"Yes\" : \"No\"", booleanField);
            fm = new FieldMetadata(booleanField, String.class);
            break;
        case Y_N:
            expression = String.format("%s ? \"Y\" : \"N\"", booleanField);
            fm = new FieldMetadata(booleanField, String.class);
            break;
        default:
            return this;
        }

        return new Node(
                builder.register(
                        new FunctionOperation(opInput(identifier), expression, new FieldList(booleanField), fm)),
                builder);
    }

    public Node apply(String expression, FieldList fieldsToApply, FieldMetadata targetField) {
        return new Node(
                builder.register(new FunctionOperation(opInput(identifier), expression, fieldsToApply, targetField)),
                builder);
    }

    public Node apply(Function<?> function, FieldList fieldsToApply, FieldMetadata targetField) {
        return apply(function, fieldsToApply, Collections.singletonList(targetField), null);
    }

    public Node apply(Function<?> function, FieldList fieldsToApply, FieldMetadata targetField,
            FieldList outputFields) {
        return apply(function, fieldsToApply, Collections.singletonList(targetField), outputFields);
    }

    public Node apply(Function<?> function, FieldList fieldsToApply, List<FieldMetadata> targetFields,
            FieldList outputFields) {
        return apply(function, fieldsToApply, targetFields, outputFields, null);
    }

    public Node apply(Function<?> function, FieldList fieldsToApply, List<FieldMetadata> targetFields,
            FieldList outputFields, Fields overrideFieldStrategy) {
        return new Node(builder.register(new FunctionOperation(opInput(identifier), function, fieldsToApply,
                targetFields, outputFields, overrideFieldStrategy)), builder);
    }

    public Node applyToAllFields(Function<?> function, List<FieldMetadata> targetFields, FieldList outputFields) {
        return new Node(
                builder.register(new FunctionOperation(opInput(identifier), function, targetFields, outputFields)),
                builder);
    }

    @Deprecated
    public Node addFunction(String expression, FieldList fieldsToApply, FieldMetadata targetField) {
        return new Node(
                builder.register(new FunctionOperation(opInput(identifier), expression, fieldsToApply, targetField)),
                builder);
    }

    @Deprecated
    public Node addFunction(String expression, FieldList fieldsToApply, FieldMetadata targetField,
            FieldList outputFields) {
        return new Node(builder.register(
                new FunctionOperation(opInput(identifier), expression, fieldsToApply, targetField, outputFields)),
                builder);
    }

    public Node addMD5(FieldList fieldsToApply, String targetFieldName) {
        return new Node(builder.addMD5(identifier, fieldsToApply, targetFieldName), builder);
    }

    // only guarantee uniqueness, not necessarily sequential
    public Node addRowID(String targetFieldName) {
        return new Node(builder.addRowId(identifier, targetFieldName), builder);
    }

    // only guarantee uniqueness, not necessarily sequential
    public Node addRowID(FieldMetadata fm) {
        return new Node(builder.addRowId(identifier, fm), builder);
    }

    public Node addTransformFunction(String packageName, TransformDefinition definition) {
        return new Node(builder.register(new TransformFunctionOperation(opInput(identifier), packageName, definition)),
                builder);
    }

    public Node renamePipe(String newname) {
        return new Node(builder.register(new RenamePipeOperation(opInput(identifier), newname)), builder);
    }

    public Node retain(FieldList outputFields) {
        return new Node(builder.register(new RetainOperation(opInput(identifier), outputFields)), builder);
    }

    public Node retain(String... outputFields) {
        return retain(new FieldList(outputFields));
    }

    public Node groupByAndRetain(FieldList outputFields, FieldList groupByFields, FieldList sortingFields) {
        return new Node(
                builder.register(new RetainOperation(opInput(identifier), outputFields, groupByFields, sortingFields)),
                builder);
    }

    public Node discard(FieldList toDiscard) {
        return new Node(builder.addDiscard(identifier, toDiscard), builder);
    }

    public Node discard(String... toDiscard) {
        return discard(new FieldList(toDiscard));
    }

    public Node checkpoint() {
        return new Node(builder.register(new CheckPointOperation(opInput(identifier))), builder);
    }

    public Node checkpoint(String name) {
        return new Node(builder.addCheckpoint(identifier, name), builder);
    }

    public Node rename(FieldList previousNames, FieldList newNames) {
        return new Node(builder.register(new RenameOperation(opInput(identifier), previousNames, newNames)), builder);
    }

    public Node stopList(Node rhs, String lhsFieldName, String rhsFieldName) {
        return new Node(builder.addStopListFilter(identifier, rhs.identifier, lhsFieldName, rhsFieldName), builder);
    }

    public Node aggregate(Aggregation aggregation) {
        return new Node(builder.register(new AggregationOperation(opInput(identifier), aggregation)), builder);
    }

    public Node combine(Node rhs) {
        return new Node(builder.addCombine(identifier, rhs.identifier), builder);
    }

    public Node merge(Node rhs) {
        return new Node(builder.register(new MergeOperation(opInput(identifier), opInput(rhs.identifier))), builder);
    }

    public Node merge(List<Node> rhs) {
        Operation.Input[] seeds = new Operation.Input[rhs.size() + 1];
        seeds[0] = opInput(identifier);
        for (int i = 0; i < rhs.size(); i++) {
            seeds[i + 1] = opInput(rhs.get(i).identifier);
        }
        return new Node(builder.register(new MergeOperation(seeds)), builder);
    }

    public Node limit(int count) {
        return new Node(builder.register(new LimitOperation(opInput(identifier), count)), builder);
    }

    public Node sample(float fraction) {
        return new Node(builder.register(new SampleOperation(opInput(identifier), fraction)), builder);
    }

    public Node addTimestamp(String timestampField, int mode) {
        return new Node(
                builder.register(
                        new AddFieldOperation(opInput(identifier), new AddTimestampStrategy(timestampField, mode))),
                builder);
    }

    public Node addTimestamp(String timestampField) {
        return new Node(
                builder.register(new AddFieldOperation(opInput(identifier), new AddTimestampStrategy(timestampField))),
                builder);
    }

    public Node addTimestamp(String timestampField, Date timestamp) {
        return new Node(builder.register(
                new AddFieldOperation(opInput(identifier), new AddTimestampStrategy(timestampField, timestamp))),
                builder);
    }

    public Node addUUID(String uuidField) {
        return new Node(builder.register(new AddFieldOperation(opInput(identifier), new AddUUIDStrategy(uuidField))),
                builder);
    }

    public Node addColumnWithFixedValue(String fieldName, Object fieldValue, Class<?> fieldType) {
        return new Node(builder.register(new AddFieldOperation(opInput(identifier),
                new AddColumnWithFixedValueStrategy(fieldName, fieldValue, fieldType))), builder);
    }

    public Node addStringColumnFromSource(String fieldName, String source) {
        return new Node(builder.register(new AddFieldOperation(opInput(identifier),
                new AddStringFieldStrategy(fieldName, source))), builder);
    }

    public Node bitEncode(String[] groupbyFields, String keyField, String valueField, String encodedField,
            BitCodeBook codeBook) {
        return new Node(builder.register(new BitEncodeOperation(opInput(identifier), groupbyFields, keyField,
                valueField, encodedField, codeBook)), builder);
    }

    public Node bitDecode(String encodedField, String[] decodeFields, BitCodeBook codeBook) {
        return new Node(
                builder.register(new BitDecodeOperation(opInput(identifier), encodedField, decodeFields, codeBook)),
                builder);
    }

    public Node kvDepivot(FieldList fieldsToAppend, FieldList fieldsNotToPivot) {
        return new Node(builder.register(new KVOperation(opInput(identifier), fieldsToAppend, fieldsNotToPivot)),
                builder);
    }

    public Node kvPickAttr(String rowIdField, KVAttrPicker picker) {
        return new Node(builder.register(new KVOperation(opInput(identifier), rowIdField, picker)), builder);
    }

    public Node kvReconstruct(String rowIdField, List<FieldMetadata> outputFields) {
        return new Node(builder.register(new KVOperation(opInput(identifier), rowIdField, outputFields)), builder);
    }

    // insert/replace literal values to every row, not insert a new row
    public Node insert(FieldList targetFields, Object... values) {
        return new Node(builder.register(new InsertOperation(opInput(identifier), targetFields, values)), builder);
    }

    // Result node only has one tuple with one field naming as outputFieldName
    public Node count(String outputFieldName) {
        Node node = addColumnWithFixedValue(outputFieldName, 1, Integer.class);
        return node.multiTierAggregate(AggregationType.SUM_LONG, outputFieldName);
    }

    /**
     * Append pre-defined log message to LE_OperationLog field.
     *
     * If LE_OperationLog does not exist yet, create it
     *
     * @param log:
     *            pre-defined log message
     * @return
     */
    public Node appendOptLog(@NotEmptyString String log) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(log), "Log should be non-empty string");
        Node node = this;
        if (this.getSchema(OperationLogUtils.DEFAULT_FIELD_NAME) == null) {
            node = node.addColumnWithFixedValue(OperationLogUtils.DEFAULT_FIELD_NAME, null, String.class);
        }
        AppendOptLogFunction func = new AppendOptLogFunction(new Fields(node.getFieldNamesArray()), log, null);
        return node.apply(func, new FieldList(node.getFieldNames()), node.getSchema(),
                new FieldList(node.getFieldNames()), Fields.REPLACE);
    }

    /**
     * Copy log from fromField and append to LE_OperationLog field.
     *
     * If LE_OperationLog does not exist yet, create it
     *
     * @param fromField
     * @return
     */
    public Node appendOptLogFromField(@NotEmptyString String fromField) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(fromField), "Log should be non-empty string");
        Preconditions.checkNotNull(this.getSchema(fromField), "Cannot find field metadata for " + fromField);
        Node node = this;
        if (this.getSchema(OperationLogUtils.DEFAULT_FIELD_NAME) == null) {
            node = node.addColumnWithFixedValue(OperationLogUtils.DEFAULT_FIELD_NAME, null, String.class);
        }
        AppendOptLogFunction func = new AppendOptLogFunction(new Fields(node.getFieldNamesArray()), null, fromField);
        return node.apply(func, new FieldList(node.getFieldNames()), node.getSchema(),
                new FieldList(node.getFieldNames()), Fields.REPLACE);
    }

    private Node multiTierAggregate(AggregationType type, String targetFieldName) {
        return multiTierAggregate(type, targetFieldName, 2);
    }

    private Node multiTierAggregate(AggregationType type, String targetFieldName, Integer tiers) {
        Node node = null;
        String dummpGroup = "__DUMMY_GROUP__";
        for (int i = tiers - 1; i >= 0; i--) {
            int groups = (int) Math.pow(10000, i);
            node = apply(new AddRandomIntFunction(dummpGroup, 1, groups, null), new FieldList(getFieldNames()),
                    new FieldMetadata(dummpGroup, Integer.class));
            List<Aggregation> aggregations = new ArrayList<>();
            aggregations.add(new Aggregation(targetFieldName, targetFieldName, type));
            node = node.groupBy(new FieldList(dummpGroup), aggregations).retain(new FieldList(targetFieldName));
        }
        return node;
    }

    public Table getSourceSchema() {
        return builder.getSourceMetadata(identifier);
    }

    public Configuration getHadoopConfig() {
        return builder.getConfig();
    }

    public Attribute getSourceAttribute(final InterfaceName interfaceName) {
        return getSourceSchema().getAttribute(interfaceName.toString());
    }

    public Attribute getSourceAttribute(String attributeName) {
        return getSourceSchema().getAttribute(attributeName);
    }

    public List<FieldMetadata> getSchema() {
        return builder.getMetadata(identifier);
    }

    public void setSchema(List<FieldMetadata> fms) {
        builder.setMetadata(identifier, fms);
    }

    public FieldMetadata getSchema(String fieldName) {
        return builder.getMetadata(identifier, fieldName);
    }

    public String getPipeName() {
        return builder.getPipeByIdentifier(identifier).getName();
    }

    public List<String> getFieldNames() {
        List<String> names = new ArrayList<>();
        List<FieldMetadata> fields = getSchema();
        for (FieldMetadata field : fields) {
            names.add(field.getFieldName());
        }
        return names;
    }

    public String[] getFieldNamesArray() {
        List<String> names = getFieldNames();
        return names.toArray(new String[names.size()]);
    }

    public String getIdentifier() {
        return identifier;
    }

    public Operation.Input opInput(String identifier) {
        Map.Entry<Pipe, List<FieldMetadata>> pipeAndMetadata = builder.getPipeAndMetadata(identifier);
        // Make a copy of the fieldmetadata so that consumers don't have to
        return new Operation.Input(pipeAndMetadata.getKey(), Lists.newArrayList(pipeAndMetadata.getValue()));
    }

}
