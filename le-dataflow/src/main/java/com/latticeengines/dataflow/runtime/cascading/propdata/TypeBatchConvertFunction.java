package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public class TypeBatchConvertFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 2717140611902104160L;

    private Map<String, Integer> namePositionMap;
    private Map<String, TypeConvertStrategy> convertTypes;
    private List<String> allFields;

    public TypeBatchConvertFunction(Fields fieldDeclaration, Map<String, TypeConvertStrategy> convertTypes,
            List<String> allFields) {
        super(fieldDeclaration);
        this.convertTypes = convertTypes;
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.allFields = allFields;
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
                        try {
                            Object objVal = arguments.getObject(entry.getKey());
                            if (objVal == null) {
                            } else {
                                String value = TypeConversionUtil.convertAnyToString(objVal);
                                result.set(loc, value);
                            }
                        } catch (Exception e) {
                            result.set(loc, Tuple.size(1));
                        }
                        break;
                    case ANY_TO_INT:
                        Object objVal = arguments.getObject(entry.getKey());
                        if (objVal == null) {
                            break;
                        }
                        try {
                            Integer intVal = TypeConversionUtil.convertAnyToInt(objVal);
                            result.set(loc, intVal);
                        } catch (Exception e) {
                            throw new UnsupportedOperationException("The target field : " + entry.getKey()
                                    + "with value : " + objVal + " cannot be casted to required type int", e);
                        }
                        break;
                    case ANY_TO_LONG:
                        objVal = arguments.getObject(entry.getKey());
                        if (objVal == null) {
                            break;
                        }
                        try {
                            Long longVal = TypeConversionUtil.convertAnyToLong(objVal);
                            result.set(loc, longVal);
                        } catch (Exception e) {
                            throw new UnsupportedOperationException("The target field : " + entry.getKey()
                                    + "with value : " + objVal + " cannot be casted to required type long", e);
                        }
                        break;
                    case ANY_TO_DOUBLE:
                        objVal = arguments.getObject(entry.getKey());
                        if (objVal == null) {
                            break;
                        }
                        try {
                            Double doubleVal = TypeConversionUtil.convertAnyToDouble(objVal);
                            result.set(loc, doubleVal);
                        } catch (Exception e) {
                            throw new UnsupportedOperationException("The target field : " + entry.getKey()
                                    + " with value : " + objVal + " cannot be casted to required type double",
                                    e);
                        }
                        break;
                    case ANY_TO_BOOLEAN:
                        objVal = arguments.getObject(entry.getKey());
                        if (objVal == null) {
                            break;
                        }
                        try {
                            Boolean booleanVal = TypeConversionUtil.convertAnyToBoolean(objVal);
                            result.set(loc, booleanVal);
                        } catch (Exception e) {
                            throw new UnsupportedOperationException("The target field : " + entry.getKey()
                                    + " with value : " + objVal + " cannot be casted to required type boolean", e);
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException("Unknown type convert strategy: " + entry.getValue());
                }
            }
        }
        functionCall.getOutputCollector().add(result);
    }
}
