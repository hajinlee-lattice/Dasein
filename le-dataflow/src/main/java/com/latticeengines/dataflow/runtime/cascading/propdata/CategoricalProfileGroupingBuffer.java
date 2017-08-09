package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class CategoricalProfileGroupingBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = 6244401115091656924L;

    private int catAttrLoc;
    private int catValueLoc;
    private String nonCatFlag;
    private int maxCat;
    private Map<String, Set<String>> catDict; // Attr -> values

    private Map<String, Integer> namePositionMap;

    public CategoricalProfileGroupingBuffer(Fields fieldDeclaration, String catAttrField, String catValueField,
            String nonCatFlag, int maxCat, List<String> catAttrs) {
        super(fieldDeclaration);
        this.nonCatFlag = nonCatFlag;
        this.maxCat = maxCat;
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.catAttrLoc = this.namePositionMap.get(catAttrField);
        this.catValueLoc = this.namePositionMap.get(catValueField);
        catDict = new HashMap<>();
        catAttrs.forEach(catAttr -> catDict.put(catAttr, new HashSet<>()));
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
        while (iter.hasNext()) {
            TupleEntry arguments = iter.next();
            processData(arguments);
        }
        outputResult(bufferCall);
    }

    private void processData(TupleEntry arguments) {
        for (Map.Entry<String, Set<String>> ent : catDict.entrySet()) {
            Set<String> knownVals = ent.getValue();
            if (knownVals.contains(nonCatFlag)) {
                continue;
            }
            String val = arguments.getString(ent.getKey());
            if (StringUtils.isBlank(val)) {
                continue;
            }
            val = StringUtils.trim(val);
            knownVals.add(val);
            if (knownVals.size() > maxCat) {
                knownVals.clear();
                knownVals.add(nonCatFlag);
            }
        }
    }

    private void outputResult(BufferCall bufferCall) {
        for (Map.Entry<String, Set<String>> ent : catDict.entrySet()) {
            Set<String> knownVals = ent.getValue();
            for (String val : knownVals) {
                Tuple result = Tuple.size(getFieldDeclaration().size());
                result.set(catAttrLoc, ent.getKey());
                result.set(catValueLoc, val);
                bufferCall.getOutputCollector().add(result);
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
