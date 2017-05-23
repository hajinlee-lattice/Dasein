package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.datacloud.dataflow.transformation.CalculateStats.BEAN_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_ATTRNAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_BKTALGO;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_BKTS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_COUNT;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_NAME;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.dataflow.utils.BucketEncodeUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.BucketConsolidateAggregator;
import com.latticeengines.dataflow.runtime.cascading.propdata.BucketExpandFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCEncodedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.Extract;

import cascading.operation.Aggregator;
import cascading.operation.Function;
import cascading.operation.aggregator.Count;
import cascading.tuple.Fields;

/**
 * This dataflow generates a list of (AttrName, AttrCount, BktCounts, BktLabels,
 * BktAlgorithm)
 */
@Component(BEAN_NAME)
public class CalculateStats extends TypesafeDataFlowBuilder<TransformationFlowParameters> {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(CalculateStats.class);

    public static final String BEAN_NAME = "calculateStats";

    private static final String BKT_ID = "_Bkt_ID_";
    private static final String BKT_COUNT = "_Bkt_Count_";

    private static final String ATTR_NAME = STATS_ATTR_NAME;
    private static final String ATTR_COUNT = STATS_ATTR_COUNT;
    private static final String ATTR_BKTS = STATS_ATTR_BKTS;

    private List<DCEncodedAttr> encAttrs;
    private Set<String> excludeAttrs;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node node1 = addSource(parameters.getBaseTables().get(0));
        Node node2 = addSource(parameters.getBaseTables().get(1));

        Node source;
        Node profile;
        if (isProfileNode(node2)) {
            log.info("Second input is the profile.");
            source = node1;
            profile = node2;
        } else if (isProfileNode(node1)) {
            log.info("First input is the profile.");
            source = node2;
            profile = node1;
        } else {
            throw new RuntimeException("Neither of the input avro has the profile schema.");
        }

        parseProfile(source, profile);

        // expand (depivot)
        Function function = new BucketExpandFunction(encAttrs, excludeAttrs, ATTR_NAME, BKT_ID);
        List<FieldMetadata> targetFields = Arrays.asList( //
                new FieldMetadata(ATTR_NAME, Integer.class), //
                new FieldMetadata(BKT_ID, Integer.class));
        Node expanded = source.apply(function, new FieldList(source.getFieldNamesArray()), targetFields,
                new FieldList(ATTR_NAME, BKT_ID));

        // group by and count
        List<FieldMetadata> countFields = new ArrayList<>(expanded.getSchema());
        countFields.add(new FieldMetadata(BKT_COUNT, Long.class));
        Node count = expanded.groupByAndAggregate(new FieldList(ATTR_NAME, BKT_ID),
                new Count(new Fields(BKT_COUNT, Long.class)), countFields, Fields.ALL);

        // consolidate count
        count = consolidateCnts(count);

        // join with profile data
        Node stats = profile.leftJoin(ATTR_NAME, count, PROFILE_ATTR_ATTRNAME);
        stats = stats.apply(String.format("%s == null ? new Long(0) : %s", ATTR_COUNT, ATTR_COUNT), //
                new FieldList(ATTR_COUNT),
                stats.getSchema(ATTR_COUNT));

        // retain
        List<String> toRetain = new ArrayList<>(count.getFieldNames());
        toRetain.add(PROFILE_ATTR_BKTALGO);
        return stats.retain(new FieldList(toRetain));
    }

    private Node consolidateCnts(Node node) {
        Aggregator aggregator = new BucketConsolidateAggregator( //
                Collections.singletonList(ATTR_NAME), BKT_ID, BKT_COUNT, ATTR_COUNT, ATTR_BKTS);
        List<FieldMetadata> outputFms = new ArrayList<>();
        outputFms.add(node.getSchema(ATTR_NAME));
        outputFms.add(new FieldMetadata(ATTR_COUNT, Long.class));
        outputFms.add(new FieldMetadata(ATTR_BKTS, String.class));
        return node.groupByAndAggregate(new FieldList(ATTR_NAME), aggregator, outputFms);
    }

    private boolean isProfileNode(Node node) {
        for (Extract extract : node.getSourceSchema().getExtracts()) {
            Iterator<GenericRecord> recordIterator = AvroUtils.iterator(node.getHadoopConfig(), extract.getPath());
            while (recordIterator.hasNext()) {
                GenericRecord record = recordIterator.next();
                return BucketEncodeUtils.isProfile(record);
            }
        }
        return false;
    }

    private void parseProfile(Node source, Node profile) {
        List<GenericRecord> records = new ArrayList<>();
        for (Extract extract : profile.getSourceSchema().getExtracts()) {
            records.addAll(AvroUtils.getDataFromGlob(profile.getHadoopConfig(), extract.getPath()));
        }
        encAttrs = BucketEncodeUtils.encodedAttrs(records);
        excludeAttrs = new HashSet<>(source.getFieldNames());
        records.forEach(record -> excludeAttrs.remove(record.get(PROFILE_ATTR_ATTRNAME).toString()));
        encAttrs.forEach(attr -> excludeAttrs.remove(attr.getEncAttr()));
    }

}
