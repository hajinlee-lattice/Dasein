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
public class ComputeRootDunsAndTypeFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -2167500142900212360L;
    private Map<String, Integer> namePositionMap;
    private String guDunsField;
    private String duDunsField;
    private String dunsField;
    private String rootDunsField;
    private String rootTypeField;
    private final static String GU_TYPE_VAL = "GU";
    private final static String DU_TYPE_VAL = "DU";
    private final static String DUNS_TYPE_VAL = "DUNS";

    public ComputeRootDunsAndTypeFunction(Fields fieldDeclaration, String guDuns, String duDuns, String duns,
            String rootDuns, String rootType) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.guDunsField = guDuns;
        this.duDunsField = duDuns;
        this.dunsField = duns;
        this.rootDunsField = rootDuns;
        this.rootTypeField = rootType;
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
        String guDunsVal = arguments.getString(guDunsField);
        String duDunsVal = arguments.getString(duDunsField);
        String dunsVal = arguments.getString(dunsField);
        if (guDunsVal != null) {
            result.set(this.namePositionMap.get(rootDunsField), guDunsVal);
            if (this.namePositionMap.get(rootTypeField) != null)
                result.set(this.namePositionMap.get(rootTypeField), GU_TYPE_VAL);
        } else if (duDunsVal != null) {
            result.set(this.namePositionMap.get(rootDunsField), duDunsVal);
            if (this.namePositionMap.get(rootTypeField) != null)
                result.set(this.namePositionMap.get(rootTypeField), DU_TYPE_VAL);
        } else if (dunsVal != null) {
            result.set(this.namePositionMap.get(rootDunsField), dunsVal);
            if (this.namePositionMap.get(rootTypeField) != null)
                result.set(this.namePositionMap.get(rootTypeField), DUNS_TYPE_VAL);
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
