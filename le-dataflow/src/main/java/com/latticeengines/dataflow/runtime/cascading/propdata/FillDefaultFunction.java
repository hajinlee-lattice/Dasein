package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings({ "rawtypes", "serial" })
public class FillDefaultFunction extends BaseOperation implements Function {
    private List<Object> defaultValues;
    private int tupleSize;

    public FillDefaultFunction(List<String> allFieldNames, List<Object> defaultValues) {
        super(new Fields(allFieldNames.toArray(new String[0])));
        getPositionMap(allFieldNames);
        this.defaultValues = defaultValues;
        this.tupleSize = defaultValues.size();
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = Tuple.size(tupleSize);
        for (int i = 0; i < tupleSize; i++) {
            Object curValue = arguments.getObject(i);
            if (curValue == null) {
                curValue = defaultValues.get(i);
            }
            result.set(i, curValue);
        }
        functionCall.getOutputCollector().add(result);
    }

    private Map<String, Integer> getPositionMap(List<String> fields) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (String field : fields) {
            positionMap.put(field, pos++);
        }
        return positionMap;
    }

}
