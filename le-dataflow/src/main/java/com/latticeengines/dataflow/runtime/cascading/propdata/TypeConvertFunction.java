package com.latticeengines.dataflow.runtime.cascading.propdata;

import com.latticeengines.domain.exposed.datacloud.dataflow.TypeConvertStrategy;
import com.latticeengines.domain.exposed.util.TypeConversionUtil;

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
    private boolean failForCastError = false;

    public TypeConvertFunction(String field, TypeConvertStrategy strategy,
            boolean failForCastError) {
        super(new Fields(field));
        this.strategy = strategy;
        this.field = field;
        this.failForCastError = failForCastError;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        switch (strategy) {
            case ANY_TO_STRING:
                Object objVal = arguments.getObject(this.field);
                String value = TypeConversionUtil.toString(objVal);
                functionCall.getOutputCollector().add(new Tuple(value));
                break;
            case ANY_TO_INT:
            case STRING_TO_INT:
                objVal = arguments.getObject(this.field);
                Integer intVal = null;
                try {
                    intVal = TypeConversionUtil.toInteger(objVal);
                } catch (Exception e) {
                    if (failForCastError) {
                        throw new RuntimeException(
                                String.format("Fail to cast field %s with value %s to Integer",
                                        this.field, TypeConversionUtil.toString(objVal)));
                    }
                }
                functionCall.getOutputCollector().add(new Tuple(intVal));
                break;
            case ANY_TO_LONG:
            case STRING_TO_LONG:
                objVal = arguments.getObject(this.field);
                Long longVal = null;
                try {
                    longVal = TypeConversionUtil.toLong(objVal);
                } catch (Exception e) {
                    if (failForCastError) {
                        throw new RuntimeException(
                                String.format("Fail to cast field %s with value %s to Long",
                                        this.field, TypeConversionUtil.toString(objVal)));
                    }
                }
                functionCall.getOutputCollector().add(new Tuple(longVal));
                break;
            case ANY_TO_DOUBLE:
                objVal = arguments.getObject(this.field);
                Double doubleVal = null;
                try {
                    doubleVal = TypeConversionUtil.toDouble(objVal);
                } catch (Exception e) {
                    if (failForCastError) {
                        throw new RuntimeException(
                                String.format("Fail to cast field %s with value %s to Double",
                                        this.field, TypeConversionUtil.toString(objVal)));
                    }
                }
                functionCall.getOutputCollector().add(new Tuple(doubleVal));
                break;
            case ANY_TO_BOOLEAN:
            case STRING_TO_BOOLEAN:
                objVal = arguments.getObject(this.field);
                Boolean booleanVal = null;
                try {
                    booleanVal = TypeConversionUtil.toBoolean(objVal);
                } catch (Exception e) {
                    if (failForCastError) {
                        throw new RuntimeException(
                                String.format("Fail to cast field %s with value %s to Boolean",
                                        this.field, TypeConversionUtil.toString(objVal)));
                    }
                }
                functionCall.getOutputCollector().add(new Tuple(booleanVal));
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unknown type convert strategy: " + this.strategy.name());
        }
    }
}
