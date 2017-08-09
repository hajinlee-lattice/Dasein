package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
public class NumericProfileSampleBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = -4518166131558866707L;

    private List<String> numAttrs;
    private Map<String, Class<?>> clsMap;
    private Map<String, List<String>> numAttrsToDecode;
    private Map<String, BitCodeBook> codeBookMap;

    private Map<String, Object> elems;
    private Map<String, Integer> seqs;

    private Map<String, Integer> namePositionMap;

    public NumericProfileSampleBuffer(Fields fieldDeclaration, List<String> numAttrs, Map<String, Class<?>> clsMap,
            Map<String, List<String>> numAttrsToDecode, Map<String, BitCodeBook> codeBookMap) {
        super(fieldDeclaration);
        this.numAttrs = numAttrs;
        this.clsMap = clsMap;
        this.numAttrsToDecode = numAttrsToDecode;
        this.codeBookMap = codeBookMap;
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        elems = new HashMap<>();
        seqs = new HashMap<>();
        for (String attr : numAttrs) {
            seqs.put(attr, 0);
        }
        for (List<String> decAttrs : numAttrsToDecode.values()) {
            decAttrs.forEach(decAttr -> seqs.put(decAttr, 0));
        }

        Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
        while (iter.hasNext()) {
            TupleEntry arguments = iter.next();
            for (String attr : numAttrs) {
                Object val = arguments.getObject(attr);
                processNumVal(attr, val);
            }
            for (Map.Entry<String, List<String>> ent : numAttrsToDecode.entrySet()) {
                String encVal = arguments.getString(ent.getKey());
                Map<String, Object> decVals = codeBookMap.get(ent.getKey()).decode(encVal, ent.getValue());
                for (Map.Entry<String, Object> decVal : decVals.entrySet()) {
                    processNumVal(decVal.getKey(), decVal.getValue());
                }
            }
        }

        for (String attr : numAttrs) {
            outputResult(bufferCall, attr);
        }

        for (Map.Entry<String, List<String>> ent : numAttrsToDecode.entrySet()) {
            for (String decAttr : ent.getValue()) {
                outputResult(bufferCall, decAttr);
            }

        }
    }

    private void outputResult(BufferCall bufferCall, String attr) {
        Tuple result = Tuple.size(getFieldDeclaration().size());
        int keyLoc = namePositionMap.get(KVDepivotStrategy.KEY_ATTR);
        result.set(keyLoc, attr);
        int valLoc = namePositionMap.get(KVDepivotStrategy.valueAttr(clsMap.get(attr)));
        result.set(valLoc, elems.get(attr));
        bufferCall.getOutputCollector().add(result);
    }

    private void processNumVal(String attr, Object val) {
        if (val == null) {
            return;
        }
        seqs.put(attr, seqs.get(attr) + 1);
        if (!elems.containsKey(attr)) {
            elems.put(attr, val);
            return;
        }
        double prob = 1.0 / seqs.get(attr);
        if (Math.random() <= prob) {
            elems.put(attr, val);
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
