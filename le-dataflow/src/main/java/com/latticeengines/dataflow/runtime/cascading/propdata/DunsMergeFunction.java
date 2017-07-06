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
public class DunsMergeFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -1474963150979757098L;
    private List<String> dunsNames;

    public DunsMergeFunction(List<String> dunsNames, String parsedDuns) {
        super(new Fields(parsedDuns));
        this.dunsNames = dunsNames;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        for (String dunsName : dunsNames) {
            try {
                String duns = arguments.getString(dunsName);
                if (StringUtils.isNotBlank(duns)) {
                    functionCall.getOutputCollector().add(new Tuple(duns));
                    return;
                }
            } catch (Exception e) {
            }
        }
        functionCall.getOutputCollector().add(Tuple.size(1));
    }
}
