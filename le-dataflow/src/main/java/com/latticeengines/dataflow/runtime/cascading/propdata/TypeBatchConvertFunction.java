package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public class TypeBatchConvertFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 2717140611902104160L;

    private Map<String, Integer> namePositionMap;
    private Map<String, TypeConvertStrategy> convertTypes;
    private List<String> allFields;
    private boolean failForCastError = false;

    public TypeBatchConvertFunction(Fields fieldDeclaration,
            Map<String, TypeConvertStrategy> convertTypes, List<String> allFields,
            boolean failForCastError) {
        super(fieldDeclaration);
        this.convertTypes = convertTypes;
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.allFields = allFields;
        this.failForCastError = failForCastError;
    }

    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName, pos++);
        }
        return positionMap;
    }

    private void setupTupleForGroup(Tuple result, TupleEntry arguments) {
        for (String field : allFields) {
            Integer loc = namePositionMap.get(field);
            if (loc != null && loc >= 0) {
                result.set(loc, arguments.getObject(field));
            }
        }
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = Tuple.size(getFieldDeclaration().size());
        setupTupleForGroup(result, arguments);
        for (Map.Entry<String, TypeConvertStrategy> entry : this.convertTypes.entrySet()) {
            Integer loc = namePositionMap.get(entry.getKey());
            if (loc != null && loc >= 0) {
                switch (entry.getValue()) {
                    case ANY_TO_STRING:
                        Object objVal = arguments.getObject(entry.getKey());
                        String value = TypeConversionUtil.toString(objVal);
                        result.set(loc, value);
                        break;
                    case ANY_TO_INT:
                        objVal = arguments.getObject(entry.getKey());
                        Integer intVal = null;
                        try {
                            intVal = TypeConversionUtil.toInteger(objVal);
                        } catch (Exception e) {
                            if (failForCastError) {
                                throw new RuntimeException(String.format(
                                        "Fail to cast field %s with value %s to Integer",
                                    entry.getKey(), TypeConversionUtil.toString(objVal)), e);
                            }
                        }
                        result.set(loc, intVal);
                        break;
                    case ANY_TO_LONG:
                        objVal = arguments.getObject(entry.getKey());
                        Long longVal = null;
                        try {
                            longVal = TypeConversionUtil.toLong(objVal);
                        } catch (Exception e) {
                            if (failForCastError) {
                                throw new RuntimeException(String.format(
                                        "Fail to cast field %s with value %s to Long",
                                    entry.getKey(), TypeConversionUtil.toString(objVal)), e);
                            }
                        }
                        result.set(loc, longVal);
                        break;
                    case ANY_TO_DOUBLE:
                        objVal = arguments.getObject(entry.getKey());
                        Double doubleVal = null;
                        try {
                            doubleVal = TypeConversionUtil.toDouble(objVal);
                        } catch (Exception e) {
                            if (failForCastError) {
                                throw new RuntimeException(String.format(
                                        "Fail to cast field %s with value %s to Double",
                                    entry.getKey(), TypeConversionUtil.toString(objVal)), e);
                            }
                        }
                        result.set(loc, doubleVal);
                        break;
                    case ANY_TO_BOOLEAN:
                        objVal = arguments.getObject(entry.getKey());
                        Boolean booleanVal = null;
                        try {
                            booleanVal = TypeConversionUtil.toBoolean(objVal);
                        } catch (Exception e) {
                            if (failForCastError) {
                                throw new RuntimeException(String.format(
                                        "Fail to cast field %s with value %s to Boolean",
                                    entry.getKey(), TypeConversionUtil.toString(objVal)), e);
                            }
                        }
                        result.set(loc, booleanVal);
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unknown type convert strategy: " + entry.getValue());
                }
            }
        }
        functionCall.getOutputCollector().add(result);
    }
}
