package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class CheckAndMapDatatypeFunction extends BaseOperation implements Function {

    private static final Logger log = LoggerFactory.getLogger(CheckAndMapDatatypeFunction.class);

    private static final long serialVersionUID = -2745833989198218174L;
    private String fieldName;
    private Type avroType;
    private List<String> errors = new ArrayList<>();

    public CheckAndMapDatatypeFunction(String fileName, Type avroType) {
        super(new Fields(fileName));
        this.fieldName = fileName;
        this.avroType = avroType;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Object value = "";
        try {
            value = arguments.getObject(fieldName);
            Object newValue = checkAndConvert(value);
            functionCall.getOutputCollector().add(new Tuple(newValue));
            return;
        } catch (Exception e) {
            if (errors.size() < 5) {
                errors.add("field=" + fieldName + " value=" + value);
            }
            if (errors.size() == 5) {
                log.warn("Failed to convert data." + errors);
            }
        }

        functionCall.getOutputCollector().add(Tuple.size(1));
    }

    private Object checkAndConvert(Object value) {
        if (value == null) {
            return null;
        }
        switch (avroType) {
        case DOUBLE:
            if (!(value instanceof Double)) {
                return Double.valueOf(value.toString());
            }
        case FLOAT:
            if (!(value instanceof Float)) {
                return Float.valueOf(value.toString());
            }
        case INT:
            if (!(value instanceof Integer)) {
                return Integer.valueOf(value.toString());
            }
        case LONG:
            if (!(value instanceof Long)) {
                return Long.valueOf(value.toString());
            }
        case BOOLEAN:
            if (!(value instanceof Boolean)) {
                return Boolean.valueOf(value.toString());
            }
        default:
            break;
        }
        return value;
    }
}
