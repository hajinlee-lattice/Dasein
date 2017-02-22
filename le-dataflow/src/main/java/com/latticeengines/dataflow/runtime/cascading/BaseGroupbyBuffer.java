package com.latticeengines.dataflow.runtime.cascading;

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

@SuppressWarnings({ "rawtypes" })
public abstract class BaseGroupbyBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = -9177444191170437390L;
    protected Map<String, Integer> namePositionMap;

    protected Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName.toLowerCase(), pos++);
        }
        return positionMap;
    }

    protected BaseGroupbyBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    @SuppressWarnings("unchecked")
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Tuple result = Tuple.size(getFieldDeclaration().size());
        TupleEntry group = bufferCall.getGroup();
        if (shouldSkipGroup(group)) {
            return;
        }
        if (shouldFlushGroup(group)) {
            flushGroup(bufferCall);
        } else {
            setupTupleForGroup(result, group);
            Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
            result = setupTupleForArgument(result, arguments);
            if (result != null) {
                bufferCall.getOutputCollector().add(result);
            }
        }
    }

    private void setupTupleForGroup(Tuple result, TupleEntry group) {
        Fields fields = group.getFields();
        for (Object field : fields) {
            String fieldName = (String) field;
            Integer loc = namePositionMap.get(fieldName.toLowerCase());
            if (loc != null && loc >= 0) {
                result.set(loc, group.getObject(fieldName));
            } else {
                System.out.println("Warning: can not find field name=" + fieldName);
            }
        }
    }

    protected boolean shouldSkipGroup(TupleEntry group) {
        return false;
    }

    protected boolean shouldFlushGroup(TupleEntry group) {
        return false;
    }

    @SuppressWarnings("unchecked")
    private void flushGroup(BufferCall bufferCall) {
        Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
        while (iter.hasNext()) {
            bufferCall.getOutputCollector().add(iter.next());
        }
    }

    protected abstract Tuple setupTupleForArgument(Tuple result, Iterator<TupleEntry> argumentsInGroup);

}
