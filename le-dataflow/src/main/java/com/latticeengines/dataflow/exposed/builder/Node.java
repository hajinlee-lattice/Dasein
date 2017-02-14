package com.latticeengines.dataflow.exposed.builder;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.latticeengines.common.exposed.query.Sort;
import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.exposed.builder.operations.AddFieldOperation;
import com.latticeengines.dataflow.exposed.builder.operations.AggregationOperation;
import com.latticeengines.dataflow.exposed.builder.operations.BitDecodeOperation;
import com.latticeengines.dataflow.exposed.builder.operations.BitEncodeOperation;
import com.latticeengines.dataflow.exposed.builder.operations.DepivotOperation;
import com.latticeengines.dataflow.exposed.builder.operations.FunctionOperation;
import com.latticeengines.dataflow.exposed.builder.operations.GroupByAndBufferOperation;
import com.latticeengines.dataflow.exposed.builder.operations.JythonFunctionOperation;
import com.latticeengines.dataflow.exposed.builder.operations.LimitOperation;
import com.latticeengines.dataflow.exposed.builder.operations.MergeOperation;
import com.latticeengines.dataflow.exposed.builder.operations.Operation;
import com.latticeengines.dataflow.exposed.builder.operations.PivotOperation;
import com.latticeengines.dataflow.exposed.builder.operations.RenameOperation;
import com.latticeengines.dataflow.exposed.builder.operations.RenamePipeOperation;
import com.latticeengines.dataflow.exposed.builder.operations.SampleOperation;
import com.latticeengines.dataflow.exposed.builder.operations.SortOperation;
import com.latticeengines.dataflow.exposed.builder.operations.TransformFunctionOperation;
import com.latticeengines.dataflow.exposed.builder.strategy.PivotStrategy;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.AddColumnWithFixedValueStrategy;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.AddTimestampStrategy;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.AddUUIDStrategy;
import com.latticeengines.domain.exposed.dataflow.BooleanType;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;

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

    public Node join(FieldList lhsJoinFields, Node rhs, FieldList rhsJoinFields, JoinType joinType) {
        return new Node(builder.addJoin(identifier, lhsJoinFields, rhs.identifier, rhsJoinFields, joinType), builder);
    }

    public Node coGroup(FieldList lhsFields, List<Node> groupNodes, List<FieldList> groupFieldLists, JoinType joinType) {

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

    public Node innerJoin(FieldList lhsJoinFields, Node rhs, FieldList rhsJoinFields) {
        Node join = new Node(builder.addInnerJoin(identifier, lhsJoinFields, rhs.identifier, rhsJoinFields), builder);

        List<String> fieldList = new ArrayList<>();
        for (FieldMetadata fm : this.getSchema()) {
            if (!fieldList.contains(fm.getFieldName())) {
                fieldList.add(fm.getFieldName());
            }
        }
        for (FieldMetadata fm : rhs.getSchema()) {
            if (!fieldList.contains(fm.getFieldName())) {
                fieldList.add(fm.getFieldName());
            }
        }

        join = join.retain(new FieldList(fieldList.toArray(new String[fieldList.size()])));
        return join;
    }

    public Node innerJoin(String lhsField, Node rhs, String rhsField) {
        return innerJoin(new FieldList(lhsField), rhs, new FieldList(rhsField));
    }

    public Node leftOuterJoin(FieldList lhsJoinFields, Node rhs, FieldList rhsJoinFields) {
        Node join = new Node(builder.addLeftOuterJoin(identifier, lhsJoinFields, rhs.identifier, rhsJoinFields),
                builder);

        List<String> fieldList = new ArrayList<>();
        for (FieldMetadata fm : this.getSchema()) {
            if (!fieldList.contains(fm.getFieldName())) {
                fieldList.add(fm.getFieldName());
            }
        }
        for (FieldMetadata fm : rhs.getSchema()) {
            if (!fieldList.contains(fm.getFieldName())) {
                fieldList.add(fm.getFieldName());
            }
        }

        join = join.retain(new FieldList(fieldList.toArray(new String[fieldList.size()])));
        return join;
    }

    public Node leftOuterJoin(String lhsField, Node rhs, String rhsField) {
        return leftOuterJoin(new FieldList(lhsField), rhs, new FieldList(rhsField));
    }

    public Node groupBy(FieldList groupByFieldList, List<Aggregation> aggregations) {
        return new Node(builder.addGroupBy(identifier, groupByFieldList, aggregations), builder);
    }

    public Node groupBy(FieldList groupByFieldList, FieldList sortFieldList, List<Aggregation> aggregations) {
        return new Node(builder.addGroupBy(identifier, groupByFieldList, sortFieldList, aggregations), builder);
    }

    public Node groupByAndLimit(FieldList groupByFieldList, int count) {
        return new Node(builder.register(new GroupByAndBufferOperation(opInput(identifier), groupByFieldList,
                new FirstNBuffer(count))), builder);
    }

    public Node groupByAndLimit(FieldList groupByFieldList, FieldList sortFieldList, int count, boolean descending,
            boolean caseInsensitive) {
        return groupByAndBuffer(groupByFieldList, sortFieldList, new FirstNBuffer(count), descending, caseInsensitive);
    }

    @SuppressWarnings("rawtypes")
    public Node groupByAndBuffer(FieldList groupByFieldList, FieldList sortFieldList, Buffer buffer, boolean descending) {
        return groupByAndBuffer(groupByFieldList, sortFieldList, buffer, descending, false);
    }

    @SuppressWarnings("rawtypes")
    public Node groupByAndBuffer(FieldList groupByFieldList, FieldList sortFieldList, Buffer buffer,
            boolean descending, boolean caseInsensitive) {
        return new Node(builder.register(new GroupByAndBufferOperation(opInput(identifier), groupByFieldList,
                sortFieldList, buffer, descending, caseInsensitive)), builder);
    }

    @SuppressWarnings("rawtypes")
    public Node groupByAndBuffer(FieldList groupByFieldList, Buffer buffer) {
        return new Node(builder.register(new GroupByAndBufferOperation(opInput(identifier), groupByFieldList, buffer)),
                builder);
    }

    @SuppressWarnings("rawtypes")
    public Node groupByAndBuffer(FieldList groupByFieldList, Buffer buffer, List<FieldMetadata> fieldMetadatas) {
        return new Node(builder.register(new GroupByAndBufferOperation(opInput(identifier), groupByFieldList, buffer,
                fieldMetadatas)), builder);
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

    public Node sort(Sort sort) {
        return new Node(builder.register(new SortOperation(opInput(identifier), sort)), builder);
    }

    public Node filter(String expression, FieldList filterFieldList) {
        return new Node(builder.addFilter(identifier, expression, filterFieldList), builder);
    }

    public Node pivot(String[] groupyByFields, PivotStrategy pivotStrategy) {
        return new Node(builder.register(new PivotOperation(opInput(identifier), groupyByFields, pivotStrategy)),
                builder);
    }

    public Node depivot(String[] targetFields, String[][] sourceFieldTuples) {
        DepivotOperation operation = new DepivotOperation(opInput(identifier), targetFields, sourceFieldTuples);
        return new Node(builder.register(operation), builder);
    }

    public Node addFunction(String expression, FieldList fieldsToApply, FieldMetadata targetField) {
        return new Node(builder.register(new FunctionOperation(opInput(identifier), expression, fieldsToApply,
                targetField)), builder);
    }

    public Node addFunction(String expression, FieldList fieldsToApply, FieldMetadata targetField,
            FieldList outputFields) {
        return new Node(builder.register(new FunctionOperation(opInput(identifier), expression, fieldsToApply,
                targetField, outputFields)), builder);
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

        return new Node(builder.register(new FunctionOperation(opInput(identifier), expression, new FieldList(
                booleanField), fm)), builder);
    }

    public Node apply(Function<?> function, FieldList fieldsToApply, FieldMetadata targetField) {
        return new Node(builder.register(new FunctionOperation(opInput(identifier), function, fieldsToApply,
                targetField)), builder);
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

    public Node addJythonFunction(String packageName, String moduleName, String functionName, FieldList fieldsToApply,
            FieldMetadata targetField) {
        return new Node(builder.register(new JythonFunctionOperation(opInput(identifier), packageName, moduleName,
                functionName, fieldsToApply, targetField)), builder);
    }

    public Node addTransformFunction(String packageName, TransformDefinition definition) {
        return new Node(builder.register(new TransformFunctionOperation(opInput(identifier), packageName, definition)),
                builder);
    }

    public Node renamePipe(String newname) {
        return new Node(builder.register(new RenamePipeOperation(opInput(identifier), newname)), builder);
    }

    public Node retain(FieldList outputFields) {
        return new Node(builder.addRetain(identifier, outputFields), builder);
    }

    public Node discard(FieldList toDiscard) {
        return new Node(builder.addDiscard(identifier, toDiscard), builder);
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

    public Node limit(int count) {
        return new Node(builder.register(new LimitOperation(opInput(identifier), count)), builder);
    }

    public Node sample(float fraction) {
        return new Node(builder.register(new SampleOperation(opInput(identifier), fraction)), builder);
    }

    public Node addTimestamp(String timestampField, int mode) {
        return new Node(builder.register(new AddFieldOperation(opInput(identifier), new AddTimestampStrategy(
                timestampField, mode))), builder);
    }

    public Node addTimestamp(String timestampField) {
        return new Node(builder.register(new AddFieldOperation(opInput(identifier), new AddTimestampStrategy(
                timestampField))), builder);
    }

    public Node addTimestamp(String timestampField, Date timestamp) {
        return new Node(builder.register(new AddFieldOperation(opInput(identifier), new AddTimestampStrategy(
                timestampField, timestamp))), builder);
    }

    public Node addUUID(String uuidField) {
        return new Node(builder.register(new AddFieldOperation(opInput(identifier), new AddUUIDStrategy(uuidField))),
                builder);
    }

    public Node addColumnWithFixedValue(String fieldName, Object fieldValue, Class<?> fieldType) {
        return new Node(builder.register(new AddFieldOperation(opInput(identifier), new AddColumnWithFixedValueStrategy(fieldName,
                fieldValue, fieldType))), builder);
    }

    public Node bitEncode(String[] groupbyFields, String keyField, String valueField, String encodedField,
            BitCodeBook codeBook) {
        return new Node(builder.register(new BitEncodeOperation(opInput(identifier), groupbyFields, keyField,
                valueField, encodedField, codeBook)), builder);
    }

    public Node bitDecode(String encodedField, String[] decodeFields, BitCodeBook codeBook) {
        return new Node(builder.register(new BitDecodeOperation(opInput(identifier), encodedField, decodeFields,
                codeBook)), builder);
    }

    public Table getSourceSchema() {
        return builder.getSourceMetadata(identifier);
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

    public String getIdentifier() {
        return identifier;
    }

    public Operation.Input opInput(String identifier) {
        Map.Entry<Pipe, List<FieldMetadata>> pipeAndMetadata = builder.getPipeAndMetadata(identifier);
        // Make a copy of the fieldmetadata so that consumers don't have to
        return new Operation.Input(pipeAndMetadata.getKey(), Lists.newArrayList(pipeAndMetadata.getValue()));
    }

    public Node hashJoin(FieldList lhsJoinFields, Node rhs, FieldList rhsJoinFields, JoinType joinType) {
        return new Node(builder.addHashJoin(identifier, lhsJoinFields, rhs.identifier, rhsJoinFields, joinType),
                builder);
    }

}
