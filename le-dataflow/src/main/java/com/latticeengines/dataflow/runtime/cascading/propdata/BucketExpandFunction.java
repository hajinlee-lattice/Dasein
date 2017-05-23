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
    private final Set<String> excludedAttrs;

    private final Map<Integer, DCEncodedAttr> encAttrArgPos = new HashMap<>();

    // expand a row into multiple records of (attrName, bktId)
    public BucketExpandFunction(List<DCEncodedAttr> encodedAttrs, Set<String> excludedAttrs, String attrNameField,
            String bktIdField) {
        super(new Fields(attrNameField, bktIdField));
        this.encodedAttrs = encodedAttrs;
        this.excludedAttrs = excludedAttrs;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        initArgPosMap(arguments);
        expandArguments(arguments, functionCall);
    }

    private void expandArguments(TupleEntry arguments, FunctionCall functionCall) {
        Fields fields = arguments.getFields();
        for (int i = 0; i < arguments.size(); i++) {
            Object value = arguments.getObject(i);
            String attrName = (String) fields.get(i);
            if (encAttrArgPos.containsKey(i)) {
                // encoded field
                DCEncodedAttr encAttr = encAttrArgPos.get(i);
                for (DCBucketedAttr bktAttr : encAttr.getBktAttrs()) {
                    int bktId = BitCodecUtils.getBits((long) value, bktAttr.getLowestBit(), bktAttr.getNumBits());
                    if (bktId > 0) {
                        Tuple tuple = new Tuple(bktAttr.getNominalAttr(), bktId);
                        functionCall.getOutputCollector().add(tuple);
                    }
                }
            } else if (!excludedAttrs.contains(attrName)) {
                // normal field
                if (value != null) {
                    Tuple tuple = new Tuple(attrName, 1);
                    functionCall.getOutputCollector().add(tuple);
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
                encAttrArgPos.putAll(map2);
            }
        }
    }

}
