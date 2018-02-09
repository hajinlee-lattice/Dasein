package com.latticeengines.dataflow.runtime.cascading.propdata;

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
    private String guDunsField;
    private String duDunsField;
    private String dunsField;
    private final static String GU_TYPE_VAL = "GU";
    private final static String DU_TYPE_VAL = "DU";
    private final static String DUNS_TYPE_VAL = "DUNS";

    public ComputeRootDunsAndTypeFunction(Fields fieldDeclaration, String guDuns, String duDuns, String duns) {
        super(fieldDeclaration);
        this.guDunsField = guDuns;
        this.duDunsField = duDuns;
        this.dunsField = duns;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = Tuple.size(getFieldDeclaration().size());
        setupTupleForArgument(result, arguments);
        functionCall.getOutputCollector().add(result);
    }

    private void setupTupleForArgument(Tuple result, TupleEntry arguments) {
        String guDunsVal = arguments.getString(guDunsField);
        String duDunsVal = arguments.getString(duDunsField);
        String dunsVal = arguments.getString(dunsField);
        if (guDunsVal != null) {
            result.set(0, guDunsVal);
            result.set(1, GU_TYPE_VAL);
        } else if (duDunsVal != null) {
            result.set(0, duDunsVal);
            result.set(1, DU_TYPE_VAL);
        } else if (dunsVal != null) {
            result.set(0, dunsVal);
            result.set(1, DUNS_TYPE_VAL);
        }
    }

}
