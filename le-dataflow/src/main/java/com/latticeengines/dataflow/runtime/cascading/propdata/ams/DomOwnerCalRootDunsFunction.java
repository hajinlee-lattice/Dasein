package com.latticeengines.dataflow.runtime.cascading.propdata.ams;

import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class DomOwnerCalRootDunsFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -2167500142900212360L;
    private static final String GU_TYPE_VAL = "GU";
    private static final String DU_TYPE_VAL = "DU";
    private static final String DUNS_TYPE_VAL = "DUNS";
    private String guDunsField = DataCloudConstants.ATTR_GU_DUNS;
    private String duDunsField = DataCloudConstants.ATTR_DU_DUNS;
    private String dunsField = DataCloudConstants.AMS_ATTR_DUNS;

    public DomOwnerCalRootDunsFunction(Fields fieldDeclaration) {
        super(fieldDeclaration);
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
