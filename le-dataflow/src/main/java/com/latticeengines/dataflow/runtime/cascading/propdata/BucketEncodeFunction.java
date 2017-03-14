package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.dataflow.exposed.builder.util.DataFlowUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.BitDecodeStrategy;
import com.latticeengines.domain.exposed.datacloud.dataflow.BooleanBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCBucketedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCEncodedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.IntervalBucket;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class BucketEncodeFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -1L;

    private static Log log = LogFactory.getLog(BucketEncodeFunction.class);

    private final Map<String, Integer> argPosMap = new HashMap<>();
    private final Map<String, Integer> namePosMap;
    private final List<DCEncodedAttr> encodedAttrs;
    private final Map<String, BitCodeBook> codeBookMap;
    private final Map<String, List<String>> colsToDecode;

    public BucketEncodeFunction(List<DCEncodedAttr> encodedAttrs, Map<String, BitCodeBook> codeBookMap) {
        super(declareFields(encodedAttrs));
        this.encodedAttrs = encodedAttrs;
        this.codeBookMap = codeBookMap;
        this.namePosMap = getPositionMap(fieldDeclaration);
        this.colsToDecode = getColsToDecode(encodedAttrs);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        initArgPosMap(arguments);
        functionCall.getOutputCollector().add(generateResult(arguments));
    }

    private static Fields declareFields(List<DCEncodedAttr> encodedAttrs) {
        List<String> encAttrNames = new ArrayList<>();
        encodedAttrs.forEach(encAttr -> encAttrNames.add(encAttr.getEncAttr()));
        return DataFlowUtils.convertToFields(encAttrNames);
    }

    protected Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName, pos++);
        }
        return positionMap;
    }

    private Map<String, List<String>> getColsToDecode(List<DCEncodedAttr> encAttrs) {
        Map<String, List<String>> colsToDecode = new HashMap<>();
        for (DCEncodedAttr encAttr: encAttrs) {
            List<DCBucketedAttr> bktAttrs = encAttr.getBktAttrs();
            for (DCBucketedAttr bktAttr : bktAttrs) {
                BitDecodeStrategy decodeStrategy = bktAttr.getDecodedStrategy();
                if (decodeStrategy == null) {
                    continue;
                }
                String codeBookKey = decodeStrategy.codeBookKey();
                if (!colsToDecode.containsKey(codeBookKey)) {
                    colsToDecode.put(codeBookKey, new ArrayList<>());
                }
                colsToDecode.get(codeBookKey).add(bktAttr.getNominalAttr());
            }
        }
        return colsToDecode;
    }

    private Tuple generateResult(TupleEntry arguments) {
        Tuple result = Tuple.size(fieldDeclaration.size());
        Map<String, Object> decodedValues = decodeAttrs(arguments);
        for (DCEncodedAttr encAttr : encodedAttrs) {
            Integer encIdx = namePosMap.get(encAttr.getEncAttr());
            long value = encode(arguments, encAttr, decodedValues);
            result.set(encIdx, value);
        }
        return result;
    }

    private void initArgPosMap(TupleEntry arguments) {
        if (argPosMap.isEmpty()) {
            Map<String, Integer> map = new HashMap<>();
            for (int i = 0; i < arguments.size(); i++) {
                String fieldName = (String) arguments.getFields().get(i);
                map.put(fieldName, i);
            }
            synchronized (argPosMap) {
                argPosMap.putAll(map);
            }
        }
    }

    private long encode(TupleEntry arguments, DCEncodedAttr encAttr, Map<String, Object> decodedValues) {
        long encoded = 0;
        List<DCBucketedAttr> bktAttrs = encAttr.getBktAttrs();
        for (DCBucketedAttr bktAttr : bktAttrs) {
            int lowestBit = bktAttr.getLowestBit();
            int numBits = bktAttr.getNumBits();
            BitDecodeStrategy decodeStrategy = bktAttr.getDecodedStrategy();
            BucketAlgorithm algo = bktAttr.getBucketAlgo();
            int bktIdx;
            if (decodeStrategy == null) {
                // simple field
                String srcArg = bktAttr.getNominalAttr();
                Object value = arguments.getObject(argPosMap.get(srcArg));
                bktIdx = bucket(value, algo);
            } else {
                bktIdx = bucket(decodedValues.get(bktAttr.getNominalAttr()), algo);
            }
            encoded = BitCodecUtils.setBits(encoded, lowestBit, numBits, bktIdx);
        }
        return encoded;
    }

    private Map<String, Object> decodeAttrs(TupleEntry arguments) {
        Map<String, Object> result = new HashMap<>();
        Map<String, Object> originalEncoded = new HashMap<>();
        for (String codeBookKey: codeBookMap.keySet()) {
            if (!originalEncoded.containsKey(codeBookKey)) {
                String encodedCol = codeBookMap.get(codeBookKey).getEncodedColumn();
                originalEncoded.put(codeBookKey, arguments.getObject(argPosMap.get(encodedCol)));
            }
        }
        for (Map.Entry<String, List<String>> entry: colsToDecode.entrySet()) {
            BitCodeBook codeBook = codeBookMap.get(entry.getKey());
            Object bitEncoded = originalEncoded.get(entry.getKey());
            if (bitEncoded != null && StringUtils.isNotBlank(bitEncoded.toString())) {
                Map<String, Object> decoded = codeBook.decode(bitEncoded.toString(), entry.getValue());
                result.putAll(decoded);
            }
        }
        return result;
    }

    private int bucket(Object value, BucketAlgorithm algo) {
        if (value == null) {
            return 0;
        }
        if (algo instanceof BooleanBucket) {
            return bucketBoolean(value);
        }
        if (algo instanceof CategoricalBucket) {
            return bucketCategorical(value, (CategoricalBucket) algo);
        }
        if (algo instanceof IntervalBucket) {
            return bucketInterval(value, (IntervalBucket) algo);
        }
        return 0;
    }

    private int bucketBoolean(Object value) {
        String str = value.toString().toLowerCase();
        if (Arrays.asList("1", "t", "true", "y", "yes").contains(str)) {
            return 1;
        } else if (Arrays.asList("0", "f", "false", "n", "no").contains(str)) {
            return 2;
        } else {
            log.warn("Cannot parse value " + value + " to a boolean");
            return 0;
        }
    }

    private int bucketCategorical(Object value, CategoricalBucket bucket) {
        List<String> categories = bucket.getCategories();
        final Map<String, String> reversedMapping = new HashMap<>();
        Map<String, List<String>> mapping = bucket.getMapping();
        if (mapping != null && !mapping.isEmpty()) {
            mapping.forEach((k, v) -> v.forEach(s -> reversedMapping.put(s, k)));
        }
        String thisCategory = value.toString();
        if (!reversedMapping.isEmpty()) {
            thisCategory = reversedMapping.get(thisCategory);
        }
        int idx = categories.indexOf(thisCategory);
        if (idx < 0) {
            log.warn("Did not find a category for value " + value + " from " + StringUtils.join(categories, ", "));
            return 0;
        } else {
            return idx + 1;
        }
    }

    private int bucketInterval(Object value, IntervalBucket bucket) {
        Number number;
        if (value instanceof Number) {
            number = (Number) value;
        } else {
            try {
                number = Double.valueOf(value.toString());
            } catch (Exception e) {
                log.error("Failed to convert value " + value + " to number for an interval bucket.");
                return 0;
            }
        }

        List<Number> boundaries = bucket.getBoundaries();
        int interval = 1;
        for (Number boundary : boundaries) {
            if (boundary.doubleValue() <= number.doubleValue()) {
                interval++;
            } else {
                break;
            }
        }
        return interval;
    }

}
