package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings({ "rawtypes", "serial" })
public class ConsolidateDataFuction extends BaseOperation implements Function {
    private Map<String, Integer> namePositionMap;
    private List<String> allFieldNames;
    private Set<String> commonFields;
    private Map<String, Map<String, String>> dupeFieldMap;

    public ConsolidateDataFuction(List<String> allFieldNames, Set<String> commonFields,
            Map<String, Map<String, String>> dupeFieldMap) {
        super(new Fields(allFieldNames.toArray(new String[0])));
        this.allFieldNames = allFieldNames;
        this.commonFields = commonFields;
        this.namePositionMap = getPositionMap(allFieldNames);
        this.dupeFieldMap = dupeFieldMap;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = Tuple.size(getFieldDeclaration().size());
        for (String fieldName : allFieldNames) {
            Integer fieldLoc = namePositionMap.get(fieldName);
            if (!commonFields.contains(fieldName)) {
                result.set(fieldLoc, arguments.getObject(fieldLoc));
            } else {
                result.set(fieldLoc, chooseDupeFieldValue(fieldName, arguments, fieldLoc));
            }
        }
        functionCall.getOutputCollector().add(result);
    }

    private Object chooseDupeFieldValue(String fieldName, TupleEntry arguments, Integer fieldLoc) {
        Object value = arguments.getObject(fieldLoc);
        if (value != null) {
            return value;
        }
        for (Map.Entry<String, Map<String, String>> entry : dupeFieldMap.entrySet()) {
            Map<String, String> dupeFieldMapPerTable = entry.getValue();
            if (dupeFieldMapPerTable.containsKey(fieldName)) {
                String dupeFieldName = dupeFieldMapPerTable.get(fieldName);
                value = arguments.getObject(namePositionMap.get(dupeFieldName));
                if (value != null) {
                    return value;
                }
            }
        }
        return value;
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
