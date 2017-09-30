package com.latticeengines.dataflow.runtime.cascading.propdata;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.datacloud.dataflow.TypeConvertStrategy;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class TypeConvertFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 2717140611902104160L;

    private TypeConvertStrategy strategy;
    private String field;

    public TypeConvertFunction(String field, TypeConvertStrategy strategy) {
        super(new Fields(field));
        this.strategy = strategy;
        this.field = field;
    }
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        switch (strategy) {
        case LONG_TO_STRING:
            try {
                String value = arguments.getLong(this.field) + "";
                functionCall.getOutputCollector().add(new Tuple(value));
            } catch (Exception e) {
                functionCall.getOutputCollector().add(Tuple.size(1));
            }
            break;
        case STRING_TO_INT:
            try {
                Integer value = Integer.parseInt(arguments.getString(this.field));
                functionCall.getOutputCollector().add(new Tuple(value));
            } catch (Exception e) {
                functionCall.getOutputCollector().add(Tuple.size(1));
            }
            break;
        case STRING_TO_LONG:
            try {
                Long value = Long.parseLong(arguments.getString(this.field));
                functionCall.getOutputCollector().add(new Tuple(value));
            } catch (Exception e) {
                functionCall.getOutputCollector().add(Tuple.size(1));
            }
            break;
        case STRING_TO_BOOLEAN:
            String value = arguments.getString(this.field);
            if (StringUtils.isEmpty(value)) {
                functionCall.getOutputCollector().add(Tuple.size(1));
            } else if (value.equalsIgnoreCase("Y") || value.equalsIgnoreCase("YES") || value.equalsIgnoreCase("TRUE")
                    || value.equalsIgnoreCase("1")) {
                functionCall.getOutputCollector().add(new Tuple(Boolean.TRUE));
            } else if (value.equalsIgnoreCase("N") || value.equalsIgnoreCase("NO") || value.equalsIgnoreCase("FALSE")
                    || value.equalsIgnoreCase("0")) {
                functionCall.getOutputCollector().add(new Tuple(Boolean.FALSE));
            } else {
                functionCall.getOutputCollector().add(Tuple.size(1));
            }
            break;
        default:
            throw new UnsupportedOperationException("Unknown type convert strategy: " + this.strategy.name());
        }
    }
}
