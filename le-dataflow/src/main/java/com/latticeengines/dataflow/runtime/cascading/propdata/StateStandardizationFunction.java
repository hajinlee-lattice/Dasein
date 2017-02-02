package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.common.exposed.util.LocationUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class StateStandardizationFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -3540875319501345171L;

    private Map<String, Integer> namePositionMap;
    private String standardCountryField;
    private String stateField;
    private int stateLoc;

    public StateStandardizationFunction(Fields fieldDeclaration, String standardCountryField, String stateField) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.standardCountryField = standardCountryField;
        this.stateField = stateField;
        this.stateLoc = namePositionMap.get(stateField);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String standardCountry = arguments.getString(standardCountryField);
        String state = arguments.getString(stateField);
        state = LocationUtils.getStandardState(standardCountry, state);
        Tuple result = Tuple.size(getFieldDeclaration().size());
        setupTupleForGroup(result, arguments);
        result.set(stateLoc, state);
        functionCall.getOutputCollector().add(result);
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

    private void setupTupleForGroup(Tuple result, TupleEntry arguments) {
        for (Object field : getFieldDeclaration()) {
            String fieldName = (String) field;
            Integer loc = namePositionMap.get(fieldName);
            if (loc != null && loc >= 0) {
                result.set(loc, arguments.getObject(fieldName));
            }
        }
    }
}
