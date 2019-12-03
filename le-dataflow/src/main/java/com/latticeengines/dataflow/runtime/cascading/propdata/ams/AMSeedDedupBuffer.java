package com.latticeengines.dataflow.runtime.cascading.propdata.ams;

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AMSeedDedupBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = 6748167081827932846L;
    private String checkingField;

    public AMSeedDedupBuffer(Fields fieldDeclaration, String checkingField) {
        super(fieldDeclaration);
        this.checkingField = checkingField;
    }

    @SuppressWarnings("unchecked")
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        TupleEntry group = bufferCall.getGroup();
        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        setupTupleForArgument(bufferCall, arguments, group);
    }

    @SuppressWarnings("unchecked")
    private void setupTupleForArgument(BufferCall bufferCall, Iterator<TupleEntry> argumentsInGroup,
            TupleEntry group) {
        boolean dropPendingEntry = false;
        Iterator<TupleEntry> entries = bufferCall.getArgumentsIterator();
        Tuple pendingEntry = null;
        while (entries.hasNext()) {
            TupleEntry arguments = entries.next();
            Object checkFieldValue = arguments.getObject(checkingField);
            if (checkFieldValue == null) {
                pendingEntry = arguments.getTuple();
            } else {
                dropPendingEntry = true;
                bufferCall.getOutputCollector().add(arguments.getTuple());
            }
        }
        if (!dropPendingEntry) {
            bufferCall.getOutputCollector().add(pendingEntry);
        }
    }
}
