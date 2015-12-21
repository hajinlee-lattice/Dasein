package com.latticeengines.propdata.collection.dataflow.buffer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class MostRecentBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = -5692917328708255966L;
    protected String timestampField;
    protected Map<String, Integer> namePositionMap;

    protected MostRecentBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    public MostRecentBuffer(String timestampField, Fields fieldDeclaration) {
        this(fieldDeclaration);
        this.timestampField = timestampField;
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
        Comparable latestTimestamp = null;
        while (arguments.hasNext()) {
            TupleEntry argument = arguments.next();
            if (shouldUpdateResult(latestTimestamp, argument)) {
                latestTimestamp = updateTupleAndReturnTimestamp(result, argument);
            }
        }

        if (shouldReturn(result)) {
            bufferCall.getOutputCollector().add(result);
        }
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

    private Comparable updateTupleAndReturnTimestamp(Tuple result, TupleEntry tupleEntry) {
        Fields fields = tupleEntry.getFields();
        Comparable timestamp = null;
        for (Object field : fields) {
            String fieldName = (String) field;
            Object value = tupleEntry.getObject(fieldName);
            Integer loc = namePositionMap.get(fieldName.toLowerCase());
            if (loc != null && loc >= 0) {
                if (timestampField.equalsIgnoreCase(fieldName)) {
                    timestamp = (Comparable) tupleEntry.getObject(fieldName);
                }
                result.set(loc, cleanupField(fieldName, value));
            }
        }
        return timestamp;
    }

    private boolean shouldUpdateResult(Comparable latestTimestamp, TupleEntry arguments) {
        Fields fields = arguments.getFields();
        for (Object field : fields) {
            String fieldName = (String) field;
            if (timestampField.equalsIgnoreCase(fieldName)) {
                Comparable value = (Comparable) arguments.getObject(fieldName);
                return isMostRecent(latestTimestamp, value);
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private boolean isMostRecent(Comparable latestTimestamp, Comparable timestamp) {
        return timestamp != null && (latestTimestamp == null || latestTimestamp.compareTo(timestamp) < 0);
    }

    protected Object cleanupField(String fieldName, Object value) {
        return value;
    }

    protected boolean shouldReturn(Tuple result) { return true; }

}
