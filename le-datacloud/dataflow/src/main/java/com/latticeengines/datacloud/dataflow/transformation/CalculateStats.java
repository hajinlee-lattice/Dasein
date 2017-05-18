package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.datacloud.dataflow.transformation.CalculateStats.BEAN_NAME;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.MappingFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.BucketConsolidateAggregator;
import com.latticeengines.dataflow.runtime.cascading.propdata.BucketExpandFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm;
import com.latticeengines.domain.exposed.datacloud.dataflow.CalculateStatsParameter;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCBucketedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCEncodedAttr;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.operation.Aggregator;
import cascading.operation.Function;
import cascading.operation.aggregator.Count;
import cascading.tuple.Fields;

/**
 * This dataflow generates a list of (AttrName, AttrCount, BktCounts, BktLabels, BktAlgorithm)
 */
@Component(BEAN_NAME)
public class CalculateStats extends TypesafeDataFlowBuilder<CalculateStatsParameter> {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(CalculateStats.class);

    public static final String BEAN_NAME = "calculateStats";

    private static final String ATTR_ID = "_Attr_ID_";
    private static final String BKT_ID = "_Bkt_ID_";
    private static final String BKT_COUNT = "_Bkt_Count_";

    private static final String ATTR_NAME = "AttrName";
    private static final String ATTR_COUNT = "AttrCount";
    private static final String ATTR_BKTS = "BktCounts";
    private static final String BKT_LBLS = "BktLabels";
    private static final String BKT_ALGO = "BktAlgorithm";


    @Override
    public Node construct(CalculateStatsParameter parameters) {
        Node am = addSource(parameters.getBaseTables().get(0));

        // expand (depivot)
        Map<String, Integer> attrIdMap = getAttrIdMap(parameters, am.getFieldNames());
        Function function = new BucketExpandFunction(parameters.encAttrs, attrIdMap, ATTR_ID, BKT_ID);
        List<FieldMetadata> targetFields = Arrays.asList( //
                new FieldMetadata(ATTR_ID, Integer.class), //
                new FieldMetadata(BKT_ID, Integer.class));
        Node expanded = am.apply(function, new FieldList(am.getFieldNamesArray()), targetFields,
                new FieldList(ATTR_ID, BKT_ID));

        // group by and count
        List<FieldMetadata> countFields = new ArrayList<>(expanded.getSchema());
        countFields.add(new FieldMetadata(BKT_COUNT, Long.class));
        Node count = expanded.groupByAndAggregate(new FieldList(ATTR_ID, BKT_ID),
                new Count(new Fields(BKT_COUNT, Long.class)), countFields, Fields.ALL);

        // consolidate count
        count = consolidateCnts(count);

        // TODO: join with input configuration avro

        // resume attr name
        return resumeAttrName(count, attrIdMap);
    }

    private Node consolidateCnts(Node node) {
        Aggregator aggregator = new BucketConsolidateAggregator(Collections.singletonList(ATTR_ID), BKT_ID, BKT_COUNT,
                ATTR_COUNT, ATTR_BKTS);
        List<FieldMetadata> outputFms = new ArrayList<>();
        outputFms.add(node.getSchema(ATTR_ID));
        outputFms.add(new FieldMetadata(ATTR_COUNT, Long.class));
        outputFms.add(new FieldMetadata(ATTR_BKTS, String.class));
        return node.groupByAndAggregate(new FieldList(ATTR_ID), aggregator, outputFms);
    }

    private Node appendBktAlgo(Node node, Map<String, Integer> attrIdMap, List<DCEncodedAttr> encAttrs) {
        Map<String, BucketAlgorithm> bktAlgos = new HashMap<>();
        for (DCEncodedAttr encAttr : encAttrs) {
            for (DCBucketedAttr bktAttr : encAttr.getBktAttrs()) {
                bktAlgos.put(bktAttr.getNominalAttr(), bktAttr.getBucketAlgo());
            }
        }
        Map<Serializable, Serializable> bktAlgoMap = new HashMap<>();
        attrIdMap.forEach((attrName, attrId) -> {
            if (bktAlgos.containsKey(attrName)) {
                BucketAlgorithm algo = bktAlgos.get(attrName);
                String serialized = JsonUtils.serialize(algo);
                if (algo instanceof CategoricalBucket && ((CategoricalBucket) algo).getMapping() != null
                        && !((CategoricalBucket) algo).getMapping().isEmpty()) {
                    CategoricalBucket bucket = JsonUtils.deserialize(JsonUtils.serialize(algo), CategoricalBucket.class);
                    bucket.setMapping(null);
                    serialized = JsonUtils.serialize(bucket);
                }
                bktAlgoMap.put(attrId, serialized);
            }
        });
        Function function = new MappingFunction(ATTR_ID, BKT_ALGO, bktAlgoMap);
        return node.apply(function, new FieldList(ATTR_ID), new FieldMetadata(BKT_ALGO, String.class));
    }

    private Node appendBktLabels(Node node, Map<String, Integer> attrIdMap, List<DCEncodedAttr> encAttrs) {
        Map<String, String> bktLabels = new HashMap<>();
        for (DCEncodedAttr encAttr : encAttrs) {
            for (DCBucketedAttr bktAttr : encAttr.getBktAttrs()) {
                bktLabels.put(bktAttr.getNominalAttr(), StringUtils.join(bktAttr.getBuckets(), "|"));
            }
        }
        Map<Serializable, Serializable> bktLblMap = new HashMap<>();
        attrIdMap.forEach((attrName, attrId) -> {
            if (bktLabels.containsKey(attrName)) {
                bktLblMap.put(attrId, bktLabels.get(attrName));
            }
        });
        Function function = new MappingFunction(ATTR_ID, BKT_LBLS, bktLblMap);
        return node.apply(function, new FieldList(ATTR_ID), new FieldMetadata(BKT_LBLS, String.class));
    }

    private Node resumeAttrName(Node node, Map<String, Integer> attrIdMap) {
        Map<Serializable, Serializable> attrNameMap = new HashMap<>();
        attrIdMap.forEach((s, i) -> attrNameMap.put(i, s));
        Function function = new MappingFunction(ATTR_ID, ATTR_NAME, attrNameMap);
        node = node.apply(function, new FieldList(ATTR_ID), new FieldMetadata(ATTR_NAME, String.class));
        return node.discard(ATTR_ID);
    }

    private Map<String, Integer> getAttrIdMap(CalculateStatsParameter parameters, List<String> srcFields) {
        Map<String, Integer> attrIdMap = new HashMap<>();
        int attrId = 0;
        Map<String, List<String>> bktAttrMap = expandEncAttrs(parameters.encAttrs);
        Set<String> ignoreFields = new HashSet<>(parameters.ignoreAttrs);
        for (String srcField : srcFields) {
            if (ignoreFields.contains(srcField)) {
                continue;
            }
            if (bktAttrMap.containsKey(srcField)) {
                for (String bktField : bktAttrMap.get(srcField)) {
                    attrIdMap.put(bktField, attrId++);
                }
            } else {
                attrIdMap.put(srcField, attrId++);
            }
        }
        return attrIdMap;
    }

    private Map<String, List<String>> expandEncAttrs(List<DCEncodedAttr> encAttrs) {
        Map<String, List<String>> map = new HashMap<>();
        for (DCEncodedAttr encAttr : encAttrs) {
            String encAttrName = encAttr.getEncAttr();
            List<String> bktAttrNames = new ArrayList<>();
            for (DCBucketedAttr bktAttr : encAttr.getBktAttrs()) {
                bktAttrNames.add(bktAttr.getNominalAttr());
            }
            map.put(encAttrName, bktAttrNames);
        }
        return map;
    }

}
