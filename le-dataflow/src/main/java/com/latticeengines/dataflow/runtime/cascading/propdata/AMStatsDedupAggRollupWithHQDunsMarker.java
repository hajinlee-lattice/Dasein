package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Iterator;
import java.util.List;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

@SuppressWarnings("rawtypes")
public class AMStatsDedupAggRollupWithHQDunsMarker extends BaseOperation implements Buffer {

    private static final long serialVersionUID = 4217950767704131475L;

    private Integer markerIdx = null;

    public AMStatsDedupAggRollupWithHQDunsMarker(Params parameterObject) {
        super(parameterObject.fieldDeclaration);

        int idx = 0;
        for (String field : parameterObject.fieldNames) {
            if (field.toString().equals(parameterObject.markerFieldName)) {
                markerIdx = idx;
                break;
            }
            idx++;
        }
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {

        @SuppressWarnings("unchecked")

        Iterator<TupleEntry> argumentsInGroup = bufferCall.getArgumentsIterator();
        TupleEntry firstTupleEntry = null;
        boolean hasOnlyOneTupleInGroup = true;

        TupleEntryCollector outputCollector = bufferCall.getOutputCollector();

        while (argumentsInGroup.hasNext()) {
            TupleEntry tupleEntry = argumentsInGroup.next();

            if (firstTupleEntry == null) {
                firstTupleEntry = tupleEntry;
            } else {
                // if this tuple entry is not the first one then update
                // hasOnlyOneTupleInGroup, copy tuple, update a flag in it and
                // write it to output collector

                hasOnlyOneTupleInGroup = false;
                Tuple tupleToWrite = tupleEntry.getTupleCopy();
                tupleToWrite.setBoolean(markerIdx, Boolean.TRUE);
                outputCollector.add(tupleToWrite);
            }
        }

        // now handle first tuple entry
        Tuple tupleToWrite = null;

        if (hasOnlyOneTupleInGroup) {
            tupleToWrite = firstTupleEntry.getTuple();
        } else {
            tupleToWrite = firstTupleEntry.getTupleCopy();
            tupleToWrite.setBoolean(markerIdx, Boolean.TRUE);
        }

        outputCollector.add(tupleToWrite);
    }

    public static class Params {
        Fields fieldDeclaration;
        List<String> fieldNames;
        String markerFieldName;

        public Params(Fields fieldDeclaration, //
                List<String> fieldNames, //
                String markerFieldName) {
            this.fieldDeclaration = fieldDeclaration;
            this.fieldNames = fieldNames;
            this.markerFieldName = markerFieldName;
        }
    }
}
