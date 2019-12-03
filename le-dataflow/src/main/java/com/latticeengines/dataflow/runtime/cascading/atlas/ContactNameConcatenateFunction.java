package com.latticeengines.dataflow.runtime.cascading.atlas;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class ContactNameConcatenateFunction extends BaseOperation implements Function {
    private static final long serialVersionUID = -7494994574004771238L;
    private static final Logger log = LoggerFactory.getLogger(ContactNameConcatenateFunction.class);
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
            boolean foundValue = false;
            for (String field : concatenateFields) {
                String fieldValue = null;
                try {
                    fieldValue = arguments.getString(field);
                } catch (Exception e) {
                    log.info(String.format("Field [%s] not found in %s.", field, tuple.toString()));
                }

                if (StringUtils.isNotEmpty(fieldValue)) {
                    foundValue = true;
                    sb.append(fieldValue).append(" ");
                }
            }

            if (foundValue) {
                tuple.set(positionMap.get(resultField), sb.toString().trim());
            }
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
