package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

@SuppressWarnings({ "rawtypes", "serial" })
public class AccountMasterNetNewBuffer extends BaseOperation implements Buffer {

    public AccountMasterNetNewBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Iterator<TupleEntry> entries = bufferCall.getArgumentsIterator();

        while (entries.hasNext()) {
            TupleEntry arguments = entries.next();
            bufferCall.getOutputCollector().add(arguments.getTuple());
        }
    }
}
