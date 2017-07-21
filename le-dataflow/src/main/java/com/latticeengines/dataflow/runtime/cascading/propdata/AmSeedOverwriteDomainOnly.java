package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AmSeedOverwriteDomainOnly extends BaseOperation implements Function {

    private static final long serialVersionUID = 5197866819120963909L;
    private Map<String, Integer> namePositionMap;
    private String manSeedDuns;
    private String amSeedDuns;

    public AmSeedOverwriteDomainOnly(Fields fieldDeclaration, String manSeedDuns, String amSeedDuns) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.manSeedDuns = manSeedDuns;
        this.amSeedDuns = amSeedDuns;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Fields columns = functionCall.getArgumentFields();
        Tuple result = Tuple.size(getFieldDeclaration().size());
        setupTupleForFields(result, columns);
        setupTupleForArgument(result, arguments);
        functionCall.getOutputCollector().add(result);
    }

    private void setupTupleForFields(Tuple result, Fields columns) {
        for (Object field : columns) {
            String fieldName = (String) field;
            Integer loc = namePositionMap.get(fieldName);
            if (loc != null && loc >= 0) {
                result.set(loc, fieldName);
            } else {
                System.out.println("Warning: can not find field name=" + fieldName);
            }
        }
    }

    private void setupTupleForArgument(Tuple result, TupleEntry arguments) {
        for (int i = 0; i < arguments.size(); i++) {
            result.set(i, arguments.getObject(i));
        }
        String manSeedDunsVal = arguments.getString(manSeedDuns);
        if ((!StringUtils.isEmpty(manSeedDunsVal)) || (manSeedDunsVal != null)) {
            result.set(namePositionMap.get(amSeedDuns), manSeedDunsVal);
        }
    }

    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName, pos++);
        }
        return positionMap;
    }

}
