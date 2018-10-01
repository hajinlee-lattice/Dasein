package com.latticeengines.dataflow.runtime.cascading.propdata;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.dataflow.runtime.cascading.propdata.util.TypeConversionUtil;
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
            case ANY_TO_STRING:
                Object fieldVal = arguments.getObject(this.field);
                try {
                    if (fieldVal == null) {
                        functionCall.getOutputCollector().add(Tuple.size(1));
                    } else {
                        String value = TypeConversionUtil.convertAnyToString(fieldVal);
                        functionCall.getOutputCollector().add(new Tuple(value));
                    }
                } catch (Exception e) {
                    functionCall.getOutputCollector().add(Tuple.size(1));
                }
                break;
            case ANY_TO_INT:
                fieldVal = arguments.getObject(this.field);
                if (fieldVal == null) {
                    functionCall.getOutputCollector().add(Tuple.size(1));
                    break;
                }
                String strValToInt = TypeConversionUtil.convertAnyToString(fieldVal);
                try {
                    Integer intVal;
                    if (StringUtils.countMatches(strValToInt, ".") == 1) {
                        intVal = Integer.parseInt(strValToInt.substring(0, strValToInt.indexOf(".")));
                    } else if (strValToInt.equalsIgnoreCase("true") || strValToInt.equalsIgnoreCase("false")) {
                        intVal = Boolean.valueOf(strValToInt) ? 1 : 0;
                    } else {
                        intVal = Integer.parseInt(strValToInt);
                    }
                    functionCall.getOutputCollector().add(new Tuple(intVal));
                } catch (Exception e) {
                    throw new UnsupportedOperationException("The target field : " + this.field + "with value : "
                            + strValToInt + " cannot be casted to required type int", e);
                }
                break;
            case STRING_TO_INT:
                fieldVal = arguments.getString(this.field);
                try {
                    Integer value = TypeConversionUtil.convertStringToInt(String.valueOf(fieldVal));
                    functionCall.getOutputCollector().add(new Tuple(value));
                } catch (Exception e) {
                    functionCall.getOutputCollector().add(Tuple.size(1));
                }
                break;
            case ANY_TO_LONG:
                fieldVal = arguments.getObject(this.field);
                if (fieldVal == null) {
                    functionCall.getOutputCollector().add(Tuple.size(1));
                    break;
                }
                String strValToLong = TypeConversionUtil.convertAnyToString(fieldVal);
                try {
                    Long longVal = TypeConversionUtil.convertAnyToLong(strValToLong);
                    functionCall.getOutputCollector().add(new Tuple(longVal));
                } catch (Exception e) {
                    throw new UnsupportedOperationException("The target field : " + this.field + "with value : "
                            + strValToLong + " cannot be casted to required type long", e);
                }
                break;
            case STRING_TO_LONG:
                fieldVal = arguments.getObject(this.field);
                try {
                    Long value = TypeConversionUtil.convertStringToLong(String.valueOf(fieldVal));
                    functionCall.getOutputCollector().add(new Tuple(value));
                } catch (Exception e) {
                    functionCall.getOutputCollector().add(Tuple.size(1));
                }
                break;
            case ANY_TO_DOUBLE:
                fieldVal = arguments.getObject(this.field);
                if (fieldVal == null) {
                    functionCall.getOutputCollector().add(Tuple.size(1));
                    break;
                }
                String strValToDouble = TypeConversionUtil.convertAnyToString(fieldVal);
                try {
                    Double doubleVal = TypeConversionUtil.convertAnyToDouble(strValToDouble);
                    functionCall.getOutputCollector().add(new Tuple(doubleVal));
                } catch (Exception e) {
                    throw new UnsupportedOperationException("The target field : " + this.field + " with value : "
                            + strValToDouble + " cannot be casted to required type double", e);
                }
                break;
            case ANY_TO_BOOLEAN:
                fieldVal = arguments.getObject(this.field);
                if (fieldVal == null) {
                    functionCall.getOutputCollector().add(Tuple.size(1));
                    break;
                }
                String inputVal = TypeConversionUtil.convertAnyToString(fieldVal);
                try {
                    Boolean booleanVal = TypeConversionUtil.convertAnyToBoolean(inputVal);
                    functionCall.getOutputCollector().add(new Tuple(booleanVal));
                } catch (Exception e) {
                    throw new UnsupportedOperationException("The target field : " + this.field + " with value : "
                            + inputVal + " cannot be casted to required type boolean", e);
                }
                break;
            case STRING_TO_BOOLEAN:
                String value = arguments.getString(this.field);
                if (StringUtils.isEmpty(value)) {
                    functionCall.getOutputCollector().add(Tuple.size(1));
                } else {
                    Boolean booleanVal = TypeConversionUtil.convertStringToBoolean(value);
                    if (booleanVal == null) {
                        functionCall.getOutputCollector().add(Tuple.size(1));
                    } else {
                        functionCall.getOutputCollector().add(new Tuple(booleanVal));
                    }
                }
                break;
        default:
            throw new UnsupportedOperationException("Unknown type convert strategy: " + this.strategy.name());
        }
    }
}