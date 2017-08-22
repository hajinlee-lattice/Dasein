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
public class ConsolidateAddNewColumnFuction extends BaseOperation implements Function {
    private Map<String, Integer> namePositionMap;
    private List<String> fieldNames;

    public ConsolidateAddNewColumnFuction(List<String> fieldNames, String targetField) {
        super(new Fields(targetField));
        this.namePositionMap = getPositionMap(fieldNames);
        this.fieldNames = fieldNames;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        StringBuilder result = new StringBuilder();
        for (String fieldName : fieldNames) {
            Integer fieldLoc = namePositionMap.get(fieldName);
            Object value = arguments.getObject(fieldLoc);
            if (value == null) {
                result.append("null");
            } else {
                result.append(value.toString());
            }
        }
        functionCall.getOutputCollector().add(new Tuple(result.toString()));
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
