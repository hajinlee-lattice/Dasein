package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class ValueToRangeMappingFunction extends BaseOperation implements Function {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(ValueToRangeMappingFunction.class);

    private static final long serialVersionUID = 1998532440252494334L;

    private String sourceField;
    private List<String[]> rangeValueMap;

    public ValueToRangeMappingFunction(String sourceField, String targetField, List<String[]> rangeValueMap) {
        super(1, new Fields(targetField));
        this.sourceField = sourceField;
        this.rangeValueMap = rangeValueMap;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        try {
            Double source = Double.valueOf(String.valueOf(arguments.getObject(sourceField)));
            for (String[] valueToRange : rangeValueMap) {
                Double min = StringUtils.isEmpty(valueToRange[1]) ? Double.MIN_VALUE : Double.valueOf(valueToRange[1]);
                Double max = (valueToRange.length < 3 || StringUtils.isEmpty(valueToRange[2])) ? Double.MAX_VALUE
                        : Double.valueOf(valueToRange[2]);
                if (source >= min && source <= max) {
                    functionCall.getOutputCollector().add(new Tuple(valueToRange[0]));
                    return;
                }
            }
            functionCall.getOutputCollector().add(Tuple.size(1));
        } catch (Exception e) {
            functionCall.getOutputCollector().add(Tuple.size(1));
        }
    }
}
