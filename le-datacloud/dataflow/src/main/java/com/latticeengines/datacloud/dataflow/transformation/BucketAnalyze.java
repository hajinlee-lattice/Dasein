package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.datacloud.dataflow.transformation.BucketAnalyze.BEAN_NAME;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.MappingFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.BucketExpandFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.BucketLabelSubstituteFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketAnalyzeParameter;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCBucketedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCEncodedAttr;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.operation.Function;
import cascading.operation.aggregator.Count;
import cascading.tuple.Fields;

@Component(BEAN_NAME)
public class BucketAnalyze extends TypesafeDataFlowBuilder<BucketAnalyzeParameter> {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(BucketEncode.class);

    public static final String BEAN_NAME = "bucketAnalyze";

    private static final String ATTR_ID = "_Attr_ID_";
    private static final String BKT_ID = "_Bkt_ID_";
    private static final String BKT_COUNT = "_Bkt_Count_";
    private static final String BKT_LABEL = "_Bkt_Label_";
    private static final String ATTR_NAME = "_Attr_Name_";

    @Override
    public Node construct(BucketAnalyzeParameter parameters) {
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

//        // substitute bucket label
//        count = substituteBucketLabel(count, attrIdMap, parameters.encAttrs);

        // resume attr name
        return resumeAttrName(count, attrIdMap);
    }

    private Node substituteBucketLabel(Node node, Map<String, Integer> attrIdMap, List<DCEncodedAttr> encAttrs) {
        Function function = new BucketLabelSubstituteFunction(encAttrs, attrIdMap, ATTR_ID, BKT_ID, BKT_LABEL);
        node = node.apply(function, new FieldList(ATTR_ID, BKT_ID), new FieldMetadata(BKT_LABEL, String.class));
        return node.discard(BKT_ID);
    }

    private Node resumeAttrName(Node node, Map<String, Integer> attrIdMap) {
        Map<Serializable, Serializable> attrNameMap = new HashMap<>();
        attrIdMap.forEach((s, i) -> attrNameMap.put(i, s));
        Function function = new MappingFunction(ATTR_ID, ATTR_NAME, attrNameMap);
        node = node.apply(function, new FieldList(ATTR_ID), new FieldMetadata(ATTR_NAME, String.class));
        return node.discard(ATTR_ID);
    }

    private Map<String, Integer> getAttrIdMap(BucketAnalyzeParameter parameters, List<String> srcFields) {
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
