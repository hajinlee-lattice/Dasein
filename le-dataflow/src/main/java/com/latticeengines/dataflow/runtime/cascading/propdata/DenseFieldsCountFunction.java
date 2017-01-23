package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class DenseFieldsCountFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 1L;
    private List<String> denseFields;

    public DenseFieldsCountFunction(List<String> denseColumns, String countField) {
        super(new Fields(countField));
        this.denseFields = denseColumns;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        int count = 0;
        if (denseFields != null) {
            for (String field : denseFields) {
                if (StringUtils.isNotBlank(arguments.getString(field))) {
                    count++;
                }
            }
        }
        functionCall.getOutputCollector().add(new Tuple(count));
    }
}
