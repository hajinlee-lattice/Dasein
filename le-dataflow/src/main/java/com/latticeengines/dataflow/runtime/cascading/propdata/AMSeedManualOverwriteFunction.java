package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
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
public class AMSeedManualOverwriteFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -8318232337793111030L;
    private Map<String, Integer> namePositionMap;
    private String amSeedSales;
    private String amSeedTotalEmp;
    private String manualSeedSales;
    private String manualSeedTotalEmp;
    private String manualSeed_le_hq;

    public AMSeedManualOverwriteFunction(Fields fieldDeclaration, String amSeedSales, String amSeedTotalEmp,
            String manualSeedSales, String manualSeedTotalEmp, String manualSeed_le_hq) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.amSeedSales = amSeedSales;
        this.amSeedTotalEmp = amSeedTotalEmp;
        this.manualSeedSales = manualSeedSales;
        this.manualSeedTotalEmp = manualSeedTotalEmp;
        this.manualSeed_le_hq = manualSeed_le_hq;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Fields columns = functionCall.getArgumentFields();
        Tuple result = Tuple.size(getFieldDeclaration().size());
        setupTupleForFields(result, columns);
        setupTupleForArgument(result, arguments);
        functionCall.getOutputCollector().add(result);
    }

    private void setupTupleForFields(Tuple result, Fields columns) {
        for (Object field : columns) {
            String fieldName = (String) field;
            Integer loc = namePositionMap.get(fieldName);
            if (loc != null && loc >= 0) {
                result.set(loc, fieldName);
            } else {
                System.out.println("Warning: can not find field name=" + fieldName);
            }
        }
    }

    private void setupTupleForArgument(Tuple result, TupleEntry arguments) {
        for (int i = 0; i < arguments.size(); i++) {
            result.set(i, arguments.getObject(i));
        }
        Long manSeedSales = arguments.getLong(manualSeedSales);
        Integer manSeedEmp = arguments.getInteger(manualSeedTotalEmp);
        String manualSeedLeHq = arguments.getString(manualSeed_le_hq);
        if (!StringUtils.isEmpty(manualSeedLeHq)) {
            if (manSeedSales != 0L && manSeedSales != null) {
                result.set(namePositionMap.get(amSeedSales), manSeedSales);
            }
            if (manSeedEmp != 0 && manSeedEmp != null) {
                result.set(namePositionMap.get(amSeedTotalEmp), manSeedEmp);
            }
        }
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

}
