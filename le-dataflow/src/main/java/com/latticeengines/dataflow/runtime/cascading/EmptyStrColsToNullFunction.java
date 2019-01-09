package com.latticeengines.dataflow.runtime.cascading;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class EmptyStrColsToNullFunction extends BaseFunction {

    private static final long serialVersionUID = 162552196468014811L;

    public EmptyStrColsToNullFunction(Fields fieldDeclaration) {
        super(fieldDeclaration);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = arguments.getTupleCopy();
        for (Map.Entry<String, Integer> namePosMap : namePositionMap.entrySet()) {
            Integer loc = namePosMap.getValue();
            Object attributeVal = arguments.getObject(loc);
            if (attributeVal instanceof String && StringUtils.isEmpty((String) attributeVal)) {
                result.set(loc, null);
            }
        }
        functionCall.getOutputCollector().add(result);
    }
}
