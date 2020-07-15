package com.latticeengines.dataflow.runtime.cascading.propdata;

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

public class DunsValidateFunction extends BaseOperation implements Function {

    private static final Logger log = LoggerFactory.getLogger(DunsValidateFunction.class);

    private static final long serialVersionUID = -1889881348810011666L;
    private String dunsField;

    public DunsValidateFunction(String dunsField) {
        super(new Fields(dunsField));
        this.dunsField = dunsField;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String duns = arguments.getString(dunsField);
        if (!StringUtils.isEmpty(duns) && (duns.length() != 9)) {
            String errMsg = String.format("Invalid DUNS related field %s detected with value %s", dunsField, duns);
            log.error(errMsg);
            throw new RuntimeException(errMsg);
        }
        functionCall.getOutputCollector().add(new Tuple(duns));
    }
}
