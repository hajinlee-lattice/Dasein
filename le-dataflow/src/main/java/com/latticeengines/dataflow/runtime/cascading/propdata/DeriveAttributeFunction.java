package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.DeriveAttributeConfig;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings({ "rawtypes", "serial" })
public class DeriveAttributeFunction extends BaseOperation implements Function {
    List<DeriveAttributeConfig.DeriveFunc> deriveFuncs;
    List<List<Integer>> attrPositions;
    int numNewAttrs;

    public DeriveAttributeFunction(Fields fieldDeclaration,
            List<DeriveAttributeConfig.DeriveFunc> deriveFuncs) {
        super(fieldDeclaration);
        this.attrPositions = new ArrayList<List<Integer>>();
        this.numNewAttrs = fieldDeclaration.size();
        this.deriveFuncs = deriveFuncs;
        this.attrPositions = null;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        if (attrPositions == null) {
            List<List<Integer>> positions = new ArrayList<List<Integer>>();
            Fields inputFields = arguments.getFields();
            for (DeriveAttributeConfig.DeriveFunc func : deriveFuncs) {
                List<Integer> attrPosList = new ArrayList<Integer>();
                for (String attr : func.getAttributes()) {
                    attrPosList.add(new Integer(inputFields.getPos(attr)));
                }
                positions.add(attrPosList);
            }
            attrPositions = positions;
        }
        Tuple input = arguments.getTuple();
        Tuple result = Tuple.size(numNewAttrs);
        for (int i = 0; i < numNewAttrs; i++) {
            Object derivedAttr = calcOneAttr(deriveFuncs.get(i), input, attrPositions.get(i));
            result.set(i, derivedAttr);
        }
        functionCall.getOutputCollector().add(result);
    }

    private Object calcOneAttr(DeriveAttributeConfig.DeriveFunc func, Tuple data,
            List<Integer> positions) {

        if (func.getCalculation().equals(DeriveAttributeConfig.SUM)) {
            long sum = 0;
            for (Integer position : positions) {
                sum += data.getLong(position);
            }
            return new Long(sum);
        } else {
            return null;
        }
    }
}
