package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.dataflow.DCBucketedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCEncodedAttr;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class BucketLabelSubstituteFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 9085016308216893042L;

    private final String attrIdField;
    private final String bktIdField;
    private final Map<Integer, List<String>> bktLabels;

    private Integer attrIdArgPos;
    private Integer bktIdArgPos;

    // generate bucketed label
    public BucketLabelSubstituteFunction(List<DCEncodedAttr> encodedAttrs, Map<String, Integer> attrIdMap,
            String attrIdField, String bktIdField, String bktLabelField) {
        super(new Fields(bktLabelField));
        this.attrIdField = attrIdField;
        this.bktIdField = bktIdField;
        this.bktLabels = expandBucketLabels(encodedAttrs, attrIdMap);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        initArgPosMap(arguments);
        String label = getBktLabels(arguments);
        functionCall.getOutputCollector().add(new Tuple(label));
    }

    private Map<Integer, List<String>> expandBucketLabels(List<DCEncodedAttr> encodedAttrs,
            Map<String, Integer> attrIdMap) {
        Map<Integer, List<String>> map = new HashMap<>();
        List<String> generalAttrLabels = Arrays.asList(null, "not-null");

        Map<String, DCBucketedAttr> bktAttrs = new HashMap<>();
        encodedAttrs.forEach( //
                encAttr -> encAttr.getBktAttrs().forEach( //
                        bktAttr -> bktAttrs.put(bktAttr.getNominalAttr(), bktAttr)));
        attrIdMap.keySet().forEach(attrName -> {
            int attrId = attrIdMap.get(attrName);
            if (bktAttrs.containsKey(attrName)) {
                map.put(attrId, bktAttrs.get(attrName).getBuckets());
            } else {
                map.put(attrId, generalAttrLabels);
            }
        });
        return map;
    }

    private String getBktLabels(TupleEntry arguments) {
        int attrId = arguments.getInteger(attrIdArgPos);
        int bktId = arguments.getInteger(bktIdArgPos);
        return bktLabels.get(attrId).get(bktId);
    }

    private void initArgPosMap(TupleEntry arguments) {
        if (attrIdArgPos == null) {
            attrIdArgPos = arguments.getFields().getPos(attrIdField);
        }
        if (bktIdArgPos == null) {
            bktIdArgPos = arguments.getFields().getPos(bktIdField);
        }
    }
}
