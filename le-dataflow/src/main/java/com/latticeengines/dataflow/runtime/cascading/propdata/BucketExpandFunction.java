package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    private final List<DCEncodedAttr> encodedAttrs;
    private final Set<String> excludeAttrs;

    private final Map<String, Integer> attrIdMap = new HashMap<>();
    private final Map<Integer, Integer> argIdToAttrIdMap = new HashMap<>();
    private final Map<Integer, DCEncodedAttr> encAttrArgPos = new HashMap<>();

    // expand a row into multiple records of (attrId, bktId)
    public BucketExpandFunction(List<DCEncodedAttr> encodedAttrs, Set<String> excludeAttrs, String attrIdField,
                                String bktIdField) {
        super(new Fields(attrIdField, bktIdField));
        this.encodedAttrs = encodedAttrs;
        this.excludeAttrs = excludeAttrs;
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
                if (value != null) {
                    Tuple tuple = new Tuple(attrId, 1);
                    functionCall.getOutputCollector().add(tuple);
                }
            } else if (encAttrArgPos.containsKey(i)) {
                // encoded field
                DCEncodedAttr encAttr = encAttrArgPos.get(i);
                for (DCBucketedAttr bktAttr : encAttr.getBktAttrs()) {
                    int attrId = attrIdMap.get(bktAttr.getNominalAttr());
                    int bktId = BitCodecUtils.getBits((long) value, bktAttr.getLowestBit(), bktAttr.getNumBits());
                    if (bktId > 0) {
                        Tuple tuple = new Tuple(attrId, bktId);
                        functionCall.getOutputCollector().add(tuple);
                    }
                }
            }
        }
    }

    private void initArgPosMap(TupleEntry arguments) {
        if (encAttrArgPos.isEmpty()) {
            Map<Integer, DCEncodedAttr> map2 = new HashMap<>();
            for (int i = 0; i < arguments.size(); i++) {
                String fieldName = (String) arguments.getFields().get(i);
                for (DCEncodedAttr encAttr : encodedAttrs) {
                    if (encAttr.getEncAttr().equals(fieldName)) {
                        map2.put(i, encAttr);
                    }
                }
            }
            synchronized (encAttrArgPos) {
                encAttrArgPos.clear();
                encAttrArgPos.putAll(map2);
            }
        }
        if (attrIdMap.isEmpty()) {
            Map<String, Integer> map = new HashMap<>();
            int attrIdx = 0;
            for (int i = 0; i < arguments.size(); i++) {
                if (encAttrArgPos.containsKey(i)) {
                    DCEncodedAttr encAttr = encAttrArgPos.get(i);
                    for (DCBucketedAttr bktAttr: encAttr.getBktAttrs()) {
                        if (!excludeAttrs.contains(bktAttr.getNominalAttr())) {
                            map.put(bktAttr.getNominalAttr(), attrIdx++);
                        }
                    }
                } else {
                    String fieldName = (String) arguments.getFields().get(i);
                    if (!excludeAttrs.contains(fieldName)) {
                        map.put(fieldName, attrIdx++);
                    }
                }
            }
            synchronized (attrIdMap) {
                attrIdMap.clear();
                attrIdMap.putAll(map);
            }
        }
        if (argIdToAttrIdMap.isEmpty()) {
            Map<Integer, Integer> map = new HashMap<>();
            for (int i = 0; i < arguments.size(); i++) {
                String fieldName = (String) arguments.getFields().get(i);
                if (attrIdMap.containsKey(fieldName)) {
                    map.put(i, attrIdMap.get(fieldName));
                }
            }
            synchronized (argIdToAttrIdMap) {
                argIdToAttrIdMap.clear();
                argIdToAttrIdMap.putAll(map);
            }
        }

    }

}
