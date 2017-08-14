package com.latticeengines.datacloud.dataflow.transformation;

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

public class ManualSeedEnrichDunsFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 739402958076252241L;
    private Map<String, Integer> namePositionMap;
    private String manSeedDuns;
    private String manSeedNoZipDuns;
    private String manSeedNoZipCityDuns;

    public ManualSeedEnrichDunsFunction(Fields fieldDeclaration, String manSeedDuns, String manSeedNoZipDuns,
            String manSeedNoZipCityDuns) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.manSeedDuns = manSeedDuns;
        this.manSeedNoZipDuns = manSeedNoZipDuns;
        this.manSeedNoZipCityDuns = manSeedNoZipCityDuns;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Fields columns = functionCall.getArgumentFields();
        Tuple result = Tuple.size(getFieldDeclaration().size());
        setupTupleForArgument(result, arguments);
        functionCall.getOutputCollector().add(result);
    }

    private void setupTupleForArgument(Tuple result, TupleEntry arguments) {
        for (int i = 0; i < arguments.size(); i++) {
            result.set(i, arguments.getObject(i));
        }
        String mSeedDuns = arguments.getString(manSeedDuns);
        String mSeedNoZipDuns = arguments.getString(manSeedNoZipDuns);
        String mSeedNoZipCityDuns = arguments.getString(manSeedNoZipCityDuns);
        if (StringUtils.isEmpty(mSeedDuns)) {
            if (!StringUtils.isEmpty(mSeedNoZipDuns)) {
                result.set(this.namePositionMap.get(String.valueOf(manSeedDuns)), mSeedNoZipDuns);
            } else {
                result.set(this.namePositionMap.get(String.valueOf(manSeedDuns)), mSeedNoZipCityDuns);
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
