package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AttrMergeBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = -2120310421596774198L;

    public AttrMergeBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        TupleEntry result = null;
        boolean[] notNulls = null;

        TupleEntry group = bufferCall.getGroup();
        String groupValue = group.getString(0);

        Iterator<TupleEntry> argumentsIter = bufferCall.getArgumentsIterator();
        int notNullCount = 0;
        while (argumentsIter.hasNext()) {
            TupleEntry arguments = argumentsIter.next();
            if (groupValue == null) {
                bufferCall.getOutputCollector().add(arguments);
                continue;
            }
            Fields fields = arguments.getFields();
            int size = fields.size();
            if (result == null) {
                result = new TupleEntry(arguments);
                notNulls = new boolean[size];
                for (int i = 0; i < size; i++) {
                    if (arguments.getObject(i) != null) {
                        notNulls[i] = true;
                        notNullCount++;
                    }
                }
            } else {
                for (int i = 0; i < size ; i++) {
                    if (!notNulls[i]) {
                        Object obj = arguments.getObject(i);
                        if (obj != null) {
                            result.setRaw(i, obj);
                            notNulls[i] = true;
                            notNullCount++;
                        }
                    }
                }
            }
            if (notNullCount >= size) {
                break;
            }
        }
        if (groupValue != null) {
            bufferCall.getOutputCollector().add(result);
        }
    }
}
