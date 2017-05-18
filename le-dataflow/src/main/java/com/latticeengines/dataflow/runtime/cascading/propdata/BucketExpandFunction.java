package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCBucketedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCEncodedAttr;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class BucketExpandFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 2798963376075410999L;

    private final Map<String, Integer> attrIdMap;
    private final List<DCEncodedAttr> encodedAttrs;
    private final String attrIdField;
    private final String bktIdField;

    private final Map<Integer, Integer> argIdToAttrIdMap = new HashMap<>();
    private final Map<Integer, DCEncodedAttr> encAttrArgPos = new HashMap<>();

    // expand a row into multiple records of (attrId, bktId)
    public BucketExpandFunction(List<DCEncodedAttr> encodedAttrs, Map<String, Integer> attrIdMap, String attrIdField,
            String bktIdField) {
        super(new Fields(attrIdField, bktIdField));
        this.encodedAttrs = encodedAttrs;
        this.attrIdMap = attrIdMap;
        this.attrIdField = attrIdField;
        this.bktIdField = bktIdField;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        initArgPosMap(arguments);
        expandArguments(arguments, functionCall);
    }

    private void expandArguments(TupleEntry arguments, FunctionCall functionCall) {
        for (int i = 0; i < arguments.size(); i++) {
            Object value = arguments.getObject(i);
            if (argIdToAttrIdMap.containsKey(i)) {
                // normal field
                int attrId = argIdToAttrIdMap.get(i);
                int bktId = value == null ? 0 : 1;
                Tuple tuple = new Tuple(attrId, bktId);
                functionCall.getOutputCollector().add(tuple);
            } else if (encAttrArgPos.containsKey(i)) {
                // encoded field
                DCEncodedAttr encAttr = encAttrArgPos.get(i);
                for (DCBucketedAttr bktAttr: encAttr.getBktAttrs()) {
                    int attrId = attrIdMap.get(bktAttr.getNominalAttr());
                    int bktId = BitCodecUtils.getBits((long) value, bktAttr.getLowestBit(), bktAttr.getNumBits());
                    Tuple tuple = new Tuple(attrId, bktId);
                    functionCall.getOutputCollector().add(tuple);
                }
            }
        }
    }

    private void initArgPosMap(TupleEntry arguments) {
        if (argIdToAttrIdMap.isEmpty()) {
            Map<Integer, Integer> map = new HashMap<>();
            for (int i = 0; i < arguments.size(); i++) {
                String fieldName = (String) arguments.getFields().get(i);
                if (attrIdMap.containsKey(fieldName)) {
                    map.put(i, attrIdMap.get(fieldName));
                }
            }
            synchronized (argIdToAttrIdMap) {
                argIdToAttrIdMap.putAll(map);
            }
        }
        if (encAttrArgPos.isEmpty()) {
            Map<Integer, DCEncodedAttr> map2 = new HashMap<>();
            for (int i = 0; i < arguments.size(); i++) {
                String fieldName = (String) arguments.getFields().get(i);
                for (DCEncodedAttr encAttr: encodedAttrs) {
                    if (encAttr.getEncAttr().equals(fieldName)) {
                        map2.put(i, encAttr);
                    }
                }
            }
            synchronized (encAttrArgPos) {
                encAttrArgPos.putAll(map2);
            }
        }
    }

}
