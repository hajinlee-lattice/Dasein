package com.latticeengines.dataflow.runtime.cascading.propdata;

import com.latticeengines.common.exposed.util.StringStandardizationUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class DunsCleanupFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 7209881348810011270L;
    private String dunsField;

    public DunsCleanupFunction(String dunsField) {
        super(new Fields(dunsField));
        this.dunsField = dunsField;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String duns = arguments.getString(dunsField);
        String cleanDuns = StringStandardizationUtils.getStandardDuns(duns);
        functionCall.getOutputCollector().add(new Tuple(cleanDuns));
    }
}
