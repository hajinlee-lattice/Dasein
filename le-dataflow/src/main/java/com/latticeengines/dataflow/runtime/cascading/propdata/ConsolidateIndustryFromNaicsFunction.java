package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class ConsolidateIndustryFromNaicsFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 2410139712740071369L;
    private String naicsField;
    private Map<Integer, Map<Serializable, Serializable>> naicsMap;

    public ConsolidateIndustryFromNaicsFunction(String naicsField, String consolidatedIndustryField,
            Map<Integer, Map<Serializable, Serializable>> naicsMap) {
        super(1, new Fields(consolidatedIndustryField));
        this.naicsField = naicsField;
        this.naicsMap = naicsMap;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String naics = arguments.getString(naicsField);
        if (StringUtils.isEmpty(naics)) {
            functionCall.getOutputCollector().add(Tuple.size(1));
            return;
        }
        for (Map.Entry<Integer, Map<Serializable, Serializable>> entry : naicsMap.entrySet()) {
            Integer length = entry.getKey();
            Map<Serializable, Serializable> map = entry.getValue();
            if (naics.length() >= length && map.containsKey(naics.substring(0, length))) {
                functionCall.getOutputCollector().add(new Tuple(map.get(naics.substring(0, length))));
                return;
            }
        }
        functionCall.getOutputCollector().add(Tuple.size(1));
    }
}
