package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class CleanAmSeedWithDomOwnTabFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 8405618472006912324L;
    private String amSeedRootDunsField;
    private String ownTabRootDunsField;
    private String amSeedDomainField;
    private String domOwnTabDomField;
    private String amSeedDunsField;
    private Map<String, Integer> namePositionMap;

    public CleanAmSeedWithDomOwnTabFunction(Fields fieldDeclaration, String domOwnTabDomField,
            String amSeedDunsField, String amSeedDomainField, String amSeedRootDunsField,
            String ownTabRootDunsField) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.amSeedRootDunsField = amSeedRootDunsField;
        this.ownTabRootDunsField = ownTabRootDunsField;
        this.amSeedDomainField = amSeedDomainField;
        this.domOwnTabDomField = domOwnTabDomField;
        this.amSeedDunsField = amSeedDunsField;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = Tuple.size(getFieldDeclaration().size());
        setupTupleForArgument(result, arguments);
        functionCall.getOutputCollector().add(result);
    }

    private void setupTupleForArgument(Tuple result, TupleEntry arguments) {
        for (int i = 0; i < arguments.size(); i++) {
            result.set(i, arguments.getObject(i));
        }
        String amSeedRootDunsVal = arguments.getString(amSeedRootDunsField);
        String domOwnTabRootDunsVal = arguments.getString(ownTabRootDunsField);
        String amSeedDunsVal = arguments.getString(amSeedDunsField);
        String domOwnTabDomainVal = arguments.getString(domOwnTabDomField);
        if (amSeedDunsVal != null && domOwnTabDomainVal != null && domOwnTabRootDunsVal != null
                && !amSeedRootDunsVal.equals(domOwnTabRootDunsVal)) {
            result.set(this.namePositionMap.get(String.valueOf(amSeedDomainField)), null);
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
