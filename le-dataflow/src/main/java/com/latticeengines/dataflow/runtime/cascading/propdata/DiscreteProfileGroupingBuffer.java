package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.dataflow.exposed.builder.strategy.impl.KVDepivotStrategy;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class DiscreteProfileGroupingBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = 6470648672066912665L;

    private int maxDisVals;
    private List<String> numAttrs;
    private Map<String, List<String>> numAttrsToDecode;
    private Map<String, BitCodeBook> codeBookMap;

    private int disAttrLoc;
    private int disIntValueLoc;
    private int disLongValueLoc;
    private Map<String, Integer> namePositionMap;

    public DiscreteProfileGroupingBuffer(Fields fieldDeclaration, List<String> numAttrs,
            Map<String, List<String>> numAttrsToDecode, Map<String, BitCodeBook> codeBookMap, int maxDisVals) {
        super(fieldDeclaration);
        this.maxDisVals = maxDisVals;
        this.numAttrs = numAttrs;
        this.numAttrsToDecode = numAttrsToDecode;
        this.codeBookMap = codeBookMap;
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.disAttrLoc = namePositionMap.get(KVDepivotStrategy.KEY_ATTR);
        this.disIntValueLoc = namePositionMap.get(KVDepivotStrategy.valueAttr(Integer.class));
        this.disLongValueLoc = namePositionMap.get(KVDepivotStrategy.valueAttr(Long.class));
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Map<String, Set<Integer>> intDict = new HashMap<>();
        Map<String, Set<Long>> longDict = new HashMap<>();
        Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
        while (iter.hasNext()) {
            TupleEntry arguments = iter.next();
            processData(arguments, intDict, longDict);
        }
        outputResult(bufferCall, intDict, longDict);
    }

    private void processData(TupleEntry arguments, Map<String, Set<Integer>> intDict, Map<String, Set<Long>> longDict) {
        for (String attr : numAttrs) {
            Object val = arguments.getObject(attr);
            processNumVal(attr, val, intDict, longDict);
        }
        for (Map.Entry<String, List<String>> ent : numAttrsToDecode.entrySet()) {
            String encVal = arguments.getString(ent.getKey());
            Map<String, Object> decVals = codeBookMap.get(ent.getKey()).decode(encVal, ent.getValue());
            for (Map.Entry<String, Object> decVal : decVals.entrySet()) {
                processNumVal(decVal.getKey(), decVal.getValue(), intDict, longDict);
            }
        }
    }

    private void processNumVal(String attr, Object val, Map<String, Set<Integer>> intDict,
            Map<String, Set<Long>> longDict) {
        if (val == null) {
            return;
        }
        if (val instanceof Integer) {
            insertIntDict(attr, val, intDict);
        } else if (val instanceof Long) {
            insertLongDict(attr, val, longDict);
        }
    }

    private void insertIntDict(String attr, Object val, Map<String, Set<Integer>> intDict) {
        if (!intDict.containsKey(attr)) {
            intDict.put(attr, new HashSet<>());
        }
        if (intDict.get(attr).contains(null)) {
            return;
        }
        intDict.get(attr).add((Integer) val);
        if (intDict.get(attr).size() > maxDisVals) {
            intDict.get(attr).clear();
            intDict.get(attr).add(null);    // this attribute is not discrete attribute
        }
    }

    private void insertLongDict(String attr, Object val, Map<String, Set<Long>> longDict) {
        if (!longDict.containsKey(attr)) {
            longDict.put(attr, new HashSet<>());
        }
        if (longDict.get(attr).contains(null)) {
            return;
        }
        longDict.get(attr).add((Long) val);
        if (longDict.get(attr).size() > maxDisVals) {
            longDict.get(attr).clear();
            longDict.get(attr).add(null);    // this attribute is not discrete attribute
        }
    }

    private void outputResult(BufferCall bufferCall, Map<String, Set<Integer>> intDict,
            Map<String, Set<Long>> longDict) {
        for (Map.Entry<String, Set<Integer>> ent : intDict.entrySet()) {
            Set<Integer> knownVals = ent.getValue();
            if (!knownVals.contains(null)) {
                for (Integer val : knownVals) {
                    Tuple result = Tuple.size(getFieldDeclaration().size());
                    result.set(disAttrLoc, ent.getKey());
                    result.set(disIntValueLoc, val);
                    bufferCall.getOutputCollector().add(result);
                }
            }
        }
        for (Map.Entry<String, Set<Long>> ent : longDict.entrySet()) {
            Set<Long> knownVals = ent.getValue();
            if (!knownVals.contains(null)) {
                for (Long val : knownVals) {
                    Tuple result = Tuple.size(getFieldDeclaration().size());
                    result.set(disAttrLoc, ent.getKey());
                    result.set(disLongValueLoc, val);
                    bufferCall.getOutputCollector().add(result);
                }
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
