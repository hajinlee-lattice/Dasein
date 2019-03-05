package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCBucketedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCEncodedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.DateBucket;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class BucketExpandFunction extends BaseOperation implements Function {

    public static final String DIM_PREFIX = "_Dim_";
    public static final String DEDUP_PREFIX = "_Dedup_";
    private static final long serialVersionUID = 2798963376075410999L;
    private final List<DCEncodedAttr> encodedAttrs;
    private final Set<String> excludeAttrs;
    private final List<String> dimAttrs;
    private final List<String> dedupFields;
    private final Map<String, BucketAlgorithm> bucketAttrs;

    private final Map<String, Integer> attrIdMap = new HashMap<>(); // field
                                                                    // name to
                                                                    // arg pos
    private final Map<Integer, Integer> argIdToAttrIdMap = new HashMap<>(); // arg
                                                                            // pos
                                                                            // to
                                                                            // attr
                                                                            // id
    private final Map<Integer, DCEncodedAttr> encAttrArgPos = new HashMap<>();
    private final Map<Integer, BucketAlgorithm> bktAttrArgPos = new HashMap<>();

    // expand a row into multiple records of (attrId, bktId, dim ...)
    public BucketExpandFunction(List<DCEncodedAttr> encodedAttrs, Set<String> excludeAttrs,
            List<String> dimAttrs, List<String> dedupFields,
            Map<String, BucketAlgorithm> bucketAttrs, String attrIdField, String bktIdField) {
        super(constructDeclaration(dimAttrs, dedupFields, attrIdField, bktIdField));
        this.encodedAttrs = encodedAttrs;
        this.excludeAttrs = excludeAttrs;
        this.dimAttrs = dimAttrs;
        this.dedupFields = dedupFields;
        this.bucketAttrs = bucketAttrs;
    }

    private static Fields constructDeclaration(List<String> dimAttrs, List<String> dedupAttrs,
            String attrIdField, String bktIdField) {
        List<String> allFields = new ArrayList<>();
        allFields.add(attrIdField);
        allFields.add(bktIdField);
        if (CollectionUtils.isNotEmpty(dimAttrs)) {
            dimAttrs.forEach(dim -> allFields.add(DIM_PREFIX + dim));
        }
        if (CollectionUtils.isNotEmpty(dedupAttrs)) {
            dedupAttrs.forEach(f -> allFields.add(DEDUP_PREFIX + f));
        }
        return new Fields(allFields.toArray(new String[allFields.size()]));
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
                // normal field or bucket only field
                int attrId = argIdToAttrIdMap.get(i);
                int bktId = value == null ? 0 : 1;
                BucketAlgorithm algorithm = null;
                if (bktAttrArgPos.containsKey(i)) {
                    // bucket only field
                    algorithm = bktAttrArgPos.get(i);
                    bktId = BucketEncodeFunction.bucket(value, algorithm);
                }
                if (bktId > 0) {
                    if (algorithm instanceof DateBucket) {
                        // Add a tuple for all bucket IDs from the bucket this value is assigned to until the last
                        // bucket since date buckets are cumulative, and this this value should be counted in all
                        // subsequent buckets.
                        for (int j = bktId; j <= ((DateBucket) algorithm).getDateBoundaries().size() + 1; j++) {
                            Tuple tuple = constructResult(arguments, attrId, j);
                            functionCall.getOutputCollector().add(tuple);
                        }
                    } else {
                        Tuple tuple = constructResult(arguments, attrId, bktId);
                        functionCall.getOutputCollector().add(tuple);
                    }
                }
            } else if (encAttrArgPos.containsKey(i)) {
                // encoded field
                DCEncodedAttr encAttr = encAttrArgPos.get(i);
                for (DCBucketedAttr bktAttr : encAttr.getBktAttrs()) {
                    int attrId = attrIdMap.get(bktAttr.getNominalAttr());
                    int bktId = BitCodecUtils.getBits((long) value, bktAttr.getLowestBit(),
                            bktAttr.getNumBits());
                    if (bktId > 0) {
                        Tuple tuple = constructResult(arguments, attrId, bktId);
                        functionCall.getOutputCollector().add(tuple);
                    }
                }
            }
        }
    }

    private Tuple constructResult(TupleEntry arguments, int attrId, int bktId) {
        Tuple tuple = new Tuple(attrId, bktId);
        if (dimAttrs != null && !dimAttrs.isEmpty()) {
            for (String dimAttr : dimAttrs) {
                Object obj = arguments.getObject(dimAttr);
                tuple.add(obj);
            }
        }
        if (dedupFields != null && !dedupFields.isEmpty()) {
            for (String dedup : dedupFields) {
                Object obj = arguments.getObject(dedup);
                tuple.add(obj);
            }
        }
        return tuple;
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
        if (bktAttrArgPos.isEmpty()) {
            Map<Integer, BucketAlgorithm> map = new HashMap<>();
            for (int i = 0; i < arguments.size(); i++) {
                String fieldName = (String) arguments.getFields().get(i);
                if (bucketAttrs.containsKey(fieldName)) {
                    map.put(i, bucketAttrs.get(fieldName));
                }
            }
            synchronized (bktAttrArgPos) {
                bktAttrArgPos.clear();
                bktAttrArgPos.putAll(map);
            }
        }
        if (attrIdMap.isEmpty()) {
            Map<String, Integer> map = new HashMap<>();
            int attrIdx = 0;
            for (int i = 0; i < arguments.size(); i++) {
                if (encAttrArgPos.containsKey(i)) {
                    DCEncodedAttr encAttr = encAttrArgPos.get(i);
                    for (DCBucketedAttr bktAttr : encAttr.getBktAttrs()) {
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
