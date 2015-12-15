package com.latticeengines.dataflow.runtime.cascading;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
abstract public class PivotBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = -5692917328708255965L;
    protected String pivotKeyField;
    protected String pivotValueField;
    protected Map<String, Integer> namePositionMap;
    protected List<Class> fieldFormats;
    protected Map<String, String> valueColumnMap;

    protected PivotBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    public PivotBuffer(String pivotKeyField, String pivotValueField, List<Class> fieldFormats,
                       Map<String, String> valueColumnMap, Fields fieldDeclaration) {
        this(fieldDeclaration);
        this.pivotKeyField = pivotKeyField;
        this.pivotValueField = pivotValueField;
        this.valueColumnMap = valueColumnMap;
        this.fieldFormats = fieldFormats;
    }

    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field: fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName.toLowerCase(), pos++);
        }
        return positionMap;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Tuple result = Tuple.size(getFieldDeclaration().size());
        TupleEntry group = bufferCall.getGroup();
        setupTupleForGroup(result, group);

        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        while (arguments.hasNext()) {
            TupleEntry argument = arguments.next();
            setupTupleForArgument(result, argument);
        }

        bufferCall.getOutputCollector().add(result);
    }

    private void setupTupleForGroup(Tuple result, TupleEntry group) {
        Fields fields = group.getFields();
        for (Object field: fields) {
            String fieldName = (String) field;
            Integer loc = namePositionMap.get(fieldName.toLowerCase());
            if (loc != null && loc >= 0) {
                result.set(loc, group.getObject(fieldName));
            } else {
                System.out.println("Warning: can not find field name=" + fieldName);
            }
        }
    }

    private void setupTupleForArgument(Tuple result, TupleEntry tupleEntry) {
        Object value = tupleEntry.getObject(pivotValueField);
        String key = tupleEntry.getString(pivotKeyField);
        String fieldName = valueColumnMap.get(key);
        Integer loc = namePositionMap.get(fieldName.toLowerCase());
        if (loc != null && loc >= 0) {
            result.set(loc, parseArgumentValue(value, fieldFormats.get(loc)));
        } else {
            System.out.println("Warning: can not find pivot value [" + value + "]");
        }
    }

    abstract protected Object parseArgumentValue(Object value, Class type);

}
