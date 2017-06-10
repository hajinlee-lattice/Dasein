package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsDimensionUtil.ExpandedTuple;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

@SuppressWarnings("rawtypes")
public class AMStatsDimensionExpandWithHQDunsBuffer extends BaseOperation implements Buffer {
    private static final long serialVersionUID = 4217950767704131475L;

    private List<String> dimensionFields;
    private List<Integer> dimFieldPosList;
    private AMStatsDimensionUtil dimensionUtil;

    public AMStatsDimensionExpandWithHQDunsBuffer(Params parameterObject) {
        super(parameterObject.fieldDeclaration);
        dimensionUtil = new AMStatsDimensionUtil();

        this.dimensionFields = parameterObject.dimensionFields;
        dimFieldPosList = new ArrayList<>();

        for (String dimensionField : dimensionFields) {
            int idx = 0;
            for (String field : parameterObject.fields) {
                if (field.toString().equals(dimensionField)) {
                    dimFieldPosList.add(idx);
                    break;
                }
                idx++;
            }
        }
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        @SuppressWarnings("unchecked")
        Iterator<TupleEntry> argumentsInGroup = bufferCall.getArgumentsIterator();
        Fields fields = bufferCall.getArgumentFields();
        int fieldLength = fields.getComparators().length;

        ExpandedTuple resultExpandedTuple = null;

        TupleEntryCollector outputCollector = bufferCall.getOutputCollector();

        while (argumentsInGroup.hasNext()) {

            TupleEntry arguments = argumentsInGroup.next();
            Tuple originalTuple = arguments.getTuple();

            List<Long> dimValues = new ArrayList<>();
            for (Integer dimPos : dimFieldPosList) {
                dimValues.add(originalTuple.getLong(dimPos));
            }

            ExpandedTuple expandedTuple = new ExpandedTuple(originalTuple);

            if (resultExpandedTuple == null) {
                resultExpandedTuple = expandedTuple;
            } else {
                resultExpandedTuple = dimensionUtil.merge(resultExpandedTuple, expandedTuple, fieldLength);
            }
        }

        outputCollector.add(resultExpandedTuple.generateTuple());
    }

    public static class Params {
        public Fields fieldDeclaration;
        List<String> fields;
        List<String> dimensionFields;

        public Params(Fields fieldDeclaration, //
                List<String> fields, //
                List<String> dimensionFields) {
            this.fieldDeclaration = fieldDeclaration;
            this.fields = fields;
            this.dimensionFields = dimensionFields;
        }
    }
}
