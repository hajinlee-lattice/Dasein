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
import org.apache.commons.lang3.StringUtils;

public class ContactNameConcatenateFunction extends BaseOperation implements Function {
    private Map<String, Integer> positionMap;
    private List<String> concatenateFields;
    private String resultField;

    public ContactNameConcatenateFunction(Fields fieldsDeclaration, List<String> concatenateFields,
                                          String resultField) {
        super(fieldsDeclaration);
        this.positionMap = getPositionMap(fieldsDeclaration);
        this.concatenateFields = concatenateFields;
        this.resultField = resultField;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple tuple = arguments.getTupleCopy();

        if (arguments.getString(resultField) == null) {
            StringBuilder sb = new StringBuilder();
            for (String field : concatenateFields) {
                String fieldValue = arguments.getString(field);
                if (StringUtils.isNotEmpty(fieldValue)) {
                    sb.append(fieldValue).append(" ");
                }
            }
            tuple.set(positionMap.get(resultField), sb.toString().trim());
        } else {
            tuple.set(positionMap.get(resultField), arguments.getString(resultField));
        }

        functionCall.getOutputCollector().add(tuple);
    }

    private Map<String, Integer> getPositionMap(Fields fieldsDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldsDeclaration) {
            positionMap.put((String) field, pos++);
        }

        return positionMap;
    }
}
