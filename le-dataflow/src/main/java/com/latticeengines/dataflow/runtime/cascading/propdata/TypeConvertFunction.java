package com.latticeengines.dataflow.runtime.cascading.propdata;

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

    private ConvertTrategy strategy;
    private String field;

    public TypeConvertFunction(String field, ConvertTrategy strategy) {
        super(new Fields(field));
        this.strategy = strategy;
        this.field = field;
    }
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        switch (strategy) {
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
            default:
                throw new UnsupportedOperationException(
                        "Unknown type convert strategy: " + this.strategy.name());
        }
    }

    public enum ConvertTrategy {
        STRING_TO_INT, //
        STRING_TO_LONG
    }
}
