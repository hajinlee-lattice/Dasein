package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
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
    private List<String> retainFields;
    private List<String> concatenateFields;
    private String resultField;

    public ContactNameConcatenateFunction(Fields fieldsDeclaration, List<String> retainFields,
                                          List<String> concatenateFields,
                                          String resultField) {
        super(fieldsDeclaration);
        this.positionMap = getPositionMap(fieldsDeclaration);
        this.retainFields = new ArrayList<>(retainFields);
        this.concatenateFields = new ArrayList<>(concatenateFields);
        this.resultField = resultField;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        Tuple tuple = Tuple.size(getFieldDeclaration().size());
        TupleEntry arguments = functionCall.getArguments();
        for (String field : retainFields) {
            tuple.set(positionMap.get(field), arguments.getString(field));
        }

        if (StringUtils.isEmpty(arguments.getString(resultField))) {
            StringBuilder sb = new StringBuilder();
            for (String field : concatenateFields) {
                sb.append(arguments.getString(field)).append(" ");
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
