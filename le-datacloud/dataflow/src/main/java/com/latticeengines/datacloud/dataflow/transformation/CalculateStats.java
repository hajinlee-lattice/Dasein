package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.datacloud.dataflow.transformation.CalculateStats.BEAN_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_ATTRNAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_BKTALGO;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_ALGO;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_BKTS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_COUNT;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_STATS_CALCULATOR;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.dataflow.utils.BucketEncodeUtils;
import com.latticeengines.datacloud.dataflow.utils.DimensionUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.util.DataFlowUtils;
import com.latticeengines.dataflow.runtime.cascading.MappingFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.BucketConsolidateAggregator;
import com.latticeengines.dataflow.runtime.cascading.propdata.BucketExpandFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.StatsRollupAggregator;
import com.latticeengines.domain.exposed.datacloud.dataflow.AttrDimension;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCBucketedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCEncodedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.CalculateStatsConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.Extract;

import cascading.operation.Aggregator;
import cascading.operation.aggregator.Count;
import cascading.tuple.Fields;

/**
 * This dataflow generates a list of (AttrName, AttrCount, BktCounts, BktAlgorithm, Dim1, Dim2, ...)
 */
@Component(BEAN_NAME)
public class CalculateStats extends ConfigurableFlowBase<CalculateStatsConfig> {

    private static final Logger log = LoggerFactory.getLogger(CalculateStats.class);

    public static final String BEAN_NAME = "calculateStats";
    public static final String TRANSFORMER_NAME = TRANSFORMER_STATS_CALCULATOR;

    private static final String ATTR_ID = "_Attr_ID_";
    private static final String BKT_ID = "_Bkt_ID_";
    private static final String BKT_COUNT = "_Bkt_Count_";
    private static final String DIM_PREFIX = BucketExpandFunction.DIM_PREFIX;
    private static final String DEDUP_PREFIX = BucketExpandFunction.DEDUP_PREFIX;
    private static final String DEDUP_COUNT = "_Ddp_Count_";

    private static final String ATTR_NAME = STATS_ATTR_NAME;
    private static final String ATTR_COUNT = STATS_ATTR_COUNT;
    private static final String ATTR_BKTS = STATS_ATTR_BKTS;

    private List<DCEncodedAttr> encAttrs;
    private Map<String, BucketAlgorithm> bktAttrs;
    private Set<String> excludeAttrs;
    private Map<String, Integer> attrIdMap;
    private List<AttrDimension> dimensions;
    private List<String> dedupFields = new ArrayList<>();
    private Set<Integer> overlapBktAttrIds = new HashSet<>();

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node node1 = addSource(parameters.getBaseTables().get(0));
        Node node2 = addSource(parameters.getBaseTables().get(1));

        Node source;
        Node profile;
        if (BucketEncodeUtils.isProfileNode(node2)) {
            log.info("Second input is the profile.");
            source = node1;
            profile = node2;
        } else if (BucketEncodeUtils.isProfileNode(node1)) {
            log.info("First input is the profile.");
            source = node2;
            profile = node1;
        } else {
            throw new RuntimeException("Neither of the input avro has the profile schema.");
        }

        CalculateStatsConfig config = getTransformerConfig(parameters);
        if (config.getDimensionTree() != null && !config.getDimensionTree().isEmpty()) {
            extractDimensions(source, config);
        }
        if (config.getDedupFields() != null) {
            dedupFields = config.getDedupFields();
        }
        parseProfile(source, profile);

        System.out.println("Overlap Attr Ids: " + overlapBktAttrIds);

        Node count;
        if (dedupFields != null && !dedupFields.isEmpty()) {
            List<Node> nodes = splitByDedupNecessity(source);
            Node dedup = expandAndCount(nodes.get(0), "dedup", true);
            Node skip = expandAndCount(nodes.get(1), "skipdedup", false);
            count = dedup.merge(skip);
            count = mergeCounts(count);
        } else {
            count = expandAndCount(source, "count", false);
        }

        // resume attr names
        count = resumeAttrName(count);

        // join with profile data
        Node stats;
        if (config.getDimensionTree() != null && !config.getDimensionTree().isEmpty()) {
            stats = profile.innerJoin(ATTR_NAME, count, PROFILE_ATTR_ATTRNAME);
        } else {
            stats = profile.leftJoin(ATTR_NAME, count, PROFILE_ATTR_ATTRNAME);
            stats = stats.apply(String.format("%s == null ? new Long(0) : %s", ATTR_COUNT, ATTR_COUNT), //
                    new FieldList(ATTR_COUNT), stats.getSchema(ATTR_COUNT));
        }

        // retain
        List<String> toRetain = new ArrayList<>(count.getFieldNames());
        toRetain.add(STATS_ATTR_ALGO);
        if (!STATS_ATTR_ALGO.equals(PROFILE_ATTR_BKTALGO)) {
            stats = stats.rename(new FieldList(PROFILE_ATTR_BKTALGO), new FieldList(STATS_ATTR_ALGO));
        }
        stats = stats.retain(new FieldList(toRetain));

        // sort and merge to single file
        return resumeDimName(stats);
    }

    // find rows need to handle dedup
    private List<Node> splitByDedupNecessity(final Node source) {
        Node dedup = source.filter(dedupFilter(true), new FieldList(dedupFields));
        Node skip1 = source.filter(dedupFilter(false), new FieldList(dedupFields));

        List<FieldMetadata> countFields = new ArrayList<>();
        dedupFields.forEach(f -> countFields.add(source.getSchema(f)));
        countFields.add(new FieldMetadata(DEDUP_COUNT, Long.class));
        List<String> groupby = new ArrayList<>(dedupFields);
        Node dedupCnt = dedup.retain(new FieldList(dedupFields)).renamePipe("dedupcount");
        dedupCnt = dedupCnt.groupByAndAggregate(new FieldList(groupby), new Count(new Fields(DEDUP_COUNT, Long.class)), //
                countFields, Fields.ALL);

        Node join = dedup.leftJoin(new FieldList(dedupFields), dedupCnt, new FieldList(dedupFields));
        dedup = join.filter(String.format("%s != null && %s > 1", DEDUP_COUNT, DEDUP_COUNT), new FieldList(DEDUP_COUNT));
        Node skip2 = join.filter(String.format("%s == null || %s == 1", DEDUP_COUNT, DEDUP_COUNT), new FieldList(DEDUP_COUNT));
        skip2 = skip2.retain(source.getFieldNamesArray());
        Node skip = skip1.merge(skip2);
        dedup = dedup.retain(source.getFieldNamesArray());
        return Arrays.asList(dedup, skip);
    }

    private String dedupFilter(boolean shouldDedup) {
        List<String> tokens = new ArrayList<>();
        for (String field: dedupFields) {
            if (shouldDedup) {
                tokens.add("(" + field + " != null)");
            } else {
                tokens.add("(" + field + " == null)");
            }
        }
        if (shouldDedup) {
            return StringUtils.join(tokens, " && ");
        } else {
            return StringUtils.join(tokens, " || ");
        }
    }

    private Node expandAndCount(Node source, String pipename, boolean dedup) {
        // expand (depivot)
        Node expanded = depivot(source, dedup);
        // group by and count
        Node count = countBkts(expanded, dedup);
        count = consolidateCnts(count, dedup);
        // rollup dimensions
        if (dimensions != null && !dimensions.isEmpty()) {
            count = rollup(count, dedup).renamePipe(pipename);
        }
        if (dedup) {
            count = count.discard(new FieldList(getGeneratedDedupAttrs()));
        }
        return count;
    }

    private Node depivot(Node source, boolean dedup) {
        List<String> dimFields = Collections.emptyList();
        List<FieldMetadata> targetFields = new ArrayList<>(Arrays.asList( //
                new FieldMetadata(ATTR_ID, Integer.class), //
                new FieldMetadata(BKT_ID, Integer.class)));
        if (dimensions != null && !dimensions.isEmpty()) {
            // input "dim", output "_Dim_dim"
            dimFields = dimensions.stream().map(AttrDimension::getName).collect(Collectors.toList());
            dimFields.forEach(dim -> targetFields.add(new FieldMetadata(DIM_PREFIX + dim, String.class)));
        }
        List<String> inputDedupFields = new ArrayList<>();
        if (dedup) {
            dedupFields.forEach(
                    f -> targetFields.add(new FieldMetadata(DEDUP_PREFIX + f, source.getSchema(f).getJavaType())));
            inputDedupFields.addAll(dedupFields);
        }
        BucketExpandFunction function = new BucketExpandFunction(encAttrs, excludeAttrs, dimFields, inputDedupFields,
                bktAttrs,
                ATTR_ID, BKT_ID);
        List<String> fieldNames = DataFlowUtils.getFieldNames(targetFields);
        return source.applyToAllFields(function, targetFields, new FieldList(fieldNames));
    }

    private Node countBkts(Node expanded, boolean dedup) {
        List<FieldMetadata> countFields = new ArrayList<>(expanded.getSchema());
        countFields.add(new FieldMetadata(BKT_COUNT, Long.class));
        List<String> groupby = new ArrayList<>();
        groupby.add(ATTR_ID);
        groupby.add(BKT_ID);
        groupby.addAll(getGeneratedDimAttrs());
        if (dedup) {
            groupby.addAll(getGeneratedDedupAttrs());
            Node merged = expanded.groupByAndLimit(new FieldList(groupby), 1);
            merged = merged.addColumnWithFixedValue(BKT_COUNT, 1L, Long.class);
            return merged.checkpoint();
        } else {
            return expanded.groupByAndAggregate(new FieldList(groupby), new Count(new Fields(BKT_COUNT, Long.class)), //
                    countFields, Fields.ALL).checkpoint();
        }
    }

    private Node consolidateCnts(Node node, boolean dedup) {
        List<FieldMetadata> outputFms = new ArrayList<>();
        List<String> dimFields = getGeneratedDimAttrs();
        List<String> groupby = new ArrayList<>(dimFields);
        if (dedup) {
            groupby.addAll(getGeneratedDedupAttrs());
        }
        groupby.add(ATTR_ID);
        dimFields.forEach(dim -> outputFms.add(new FieldMetadata(dim, String.class)));
        if (dedup) {
            getGeneratedDedupAttrs().forEach(f -> outputFms.add(node.getSchema(f)));
        }
        outputFms.add(node.getSchema(ATTR_ID));
        outputFms.add(new FieldMetadata(ATTR_COUNT, Long.class));
        outputFms.add(new FieldMetadata(ATTR_BKTS, String.class));
        Aggregator<?> aggregator = new BucketConsolidateAggregator( //
                groupby, ATTR_ID, BKT_ID, BKT_COUNT, ATTR_COUNT, ATTR_BKTS, overlapBktAttrIds);
        return node.groupByAndAggregate(new FieldList(groupby), aggregator, outputFms);
    }

    private Node rollup(Node count, boolean dedup) {
        return bruteForceRollup(count, dedup);
    }

    private Node bruteForceRollup(Node count, boolean dedup) {
        List<List<AttrDimension>> paths = DimensionUtils.getRollupPaths(dimensions);
        if (paths.size() > 32) {
            throw new RuntimeException(paths.size()
                    + " roll up paths is too many for brute force rollup. Please simplify your dimension tree.");
        } else {
            log.info("There will be " + paths.size() + " paths to roll up.");
        }
        Map<String, Node> rollupNodes = new HashMap<>();
        List<String> fieldsInOrder = count.getFieldNames();
        for (List<AttrDimension> path : paths) {
            AttrDimension currDim = path.get(path.size() - 1);
            Node base = count;
            List<AttrDimension> parentPath = new ArrayList<>();
            if (path.size() > 1) {
                parentPath = path.subList(0, path.size() - 1);
                base = rollupNodes.get(DimensionUtils.pathToString(parentPath));
            }
            Node rollup = bruteForceRollupOneDim(base, parentPath, currDim, dedup);
            String pathName = DimensionUtils.pathToString(path);
            String pipeName = UUID.randomUUID().toString().replaceAll("-", "");
            rollup = rollup.retain(new FieldList(fieldsInOrder)).renamePipe(pipeName);
            rollupNodes.put(pathName, rollup);
        }
        if (rollupNodes.isEmpty()) {
            return count;
        } else {
            return count.merge(new ArrayList<>(rollupNodes.values()));
        }
    }

    private Node bruteForceRollupOneDim(Node base, List<AttrDimension> parentPath, AttrDimension dimension, boolean dedup) {
        List<String> dimFields = getGeneratedDimAttrs();
        List<String> parentDims = parentPath.stream().map(AttrDimension::getName).collect(Collectors.toList());
        List<String> groupby = new ArrayList<>(dimFields);
        groupby.remove(DIM_PREFIX + dimension.getName());
        parentDims.forEach(dim -> groupby.remove(DIM_PREFIX + dim));
        if (dedup) {
            groupby.addAll(getGeneratedDedupAttrs());
        }
        groupby.add(ATTR_ID);
        List<FieldMetadata> outputFms = new ArrayList<>();
        dimFields.forEach(dim -> {
            if (!parentDims.contains(dim.substring(DIM_PREFIX.length()))) {
                outputFms.add(new FieldMetadata(dim, String.class));
            }
        });
        if (dedup) {
            getGeneratedDedupAttrs().forEach(f -> outputFms.add(base.getSchema(f)));
        }
        outputFms.add(base.getSchema(ATTR_ID));
        outputFms.add(new FieldMetadata(ATTR_COUNT, Long.class));
        outputFms.add(new FieldMetadata(ATTR_BKTS, String.class));
        Aggregator<?> aggregator = new StatsRollupAggregator(groupby, dimension.getName(), ATTR_COUNT, ATTR_BKTS,
                dedup);
        Node rollup = base.groupByAndAggregate(new FieldList(groupby), aggregator, outputFms);
        for (String pd: parentDims) {
            rollup = rollup.addColumnWithFixedValue(DIM_PREFIX + pd, StatsRollupAggregator.ALL, String.class);
        }
        return rollup;
    }

    private Node mergeCounts(Node count) {
        List<String> dimFields = getGeneratedDimAttrs();
        List<String> groupby = new ArrayList<>(dimFields);
        groupby.add(ATTR_ID);
        List<FieldMetadata> outputFms = new ArrayList<>();
        dimFields.forEach(dim -> outputFms.add(new FieldMetadata(dim, String.class)));
        outputFms.add(count.getSchema(ATTR_ID));
        outputFms.add(new FieldMetadata(ATTR_COUNT, Long.class));
        outputFms.add(new FieldMetadata(ATTR_BKTS, String.class));
        StatsRollupAggregator aggregator = new StatsRollupAggregator(groupby, null, ATTR_COUNT, ATTR_BKTS, false);
        return count.groupByAndAggregate(new FieldList(groupby), aggregator, outputFms);
    }

    private Node resumeAttrName(Node node) {
        Map<Serializable, Serializable> attrNameMap = new HashMap<>();
        attrIdMap.forEach((s, i) -> attrNameMap.put(i, s));
        MappingFunction function = new MappingFunction(ATTR_ID, ATTR_NAME, attrNameMap);
        node = node.apply(function, new FieldList(ATTR_ID), new FieldMetadata(ATTR_NAME, String.class));
        node = node.discard(ATTR_ID);
        return node;
    }


    private Node resumeDimName(Node node) {
        List<String> modifiedName = getGeneratedDimAttrs();
        if (modifiedName.isEmpty()) {
            return node;
        }
        List<String> originalName = dimensions.stream().map(AttrDimension::getName).collect(Collectors.toList());
        return node.rename(new FieldList(modifiedName), new FieldList(originalName));
    }

    private void parseProfile(Node source, Node profile) {
        List<GenericRecord> records = new ArrayList<>();
        for (Extract extract : profile.getSourceSchema().getExtracts()) {
            records.addAll(AvroUtils.getDataFromGlob(profile.getHadoopConfig(), extract.getPath()));
        }
        encAttrs = BucketEncodeUtils.encodedAttrs(records);
        bktAttrs = BucketEncodeUtils.bucketFields(records);
        excludeAttrs = new HashSet<>(source.getFieldNames());
        records.forEach(record -> excludeAttrs.remove(record.get(PROFILE_ATTR_ATTRNAME).toString()));
        encAttrs.forEach(attr -> excludeAttrs.remove(attr.getEncAttr()));

        List<String> avroFieldNames = source.getFieldNames();
        Map<Integer, DCEncodedAttr> encAttrArgPos = new HashMap<>();
        for (int i = 0; i < avroFieldNames.size(); i++) {
            String fieldName = avroFieldNames.get(i);
            for (DCEncodedAttr encAttr : encAttrs) {
                if (encAttr.getEncAttr().equals(fieldName)) {
                    encAttrArgPos.put(i, encAttr);
                }
            }
        }
        attrIdMap = new HashMap<>();
        int attrIdx = 0;
        for (int i = 0; i < avroFieldNames.size(); i++) {
            if (encAttrArgPos.containsKey(i)) {
                DCEncodedAttr encAttr = encAttrArgPos.get(i);
                for (DCBucketedAttr bktAttr : encAttr.getBktAttrs()) {
                    if (!excludeAttrs.contains(bktAttr.getNominalAttr())) {
                        attrIdMap.put(bktAttr.getNominalAttr(), attrIdx++);
                    }
                }
            } else {
                String fieldName = avroFieldNames.get(i);
                if (!excludeAttrs.contains(fieldName)) {
                    attrIdMap.put(fieldName, attrIdx++);
                }
            }
        }

        List<String> overlapBktAttrs = BucketEncodeUtils.overlapBktAttrs(records);
        for (String attrName: overlapBktAttrs) {
            overlapBktAttrIds.add(attrIdMap.get(attrName));
        }
    }

    private void extractDimensions(Node source, CalculateStatsConfig config) {
        Map<String, List<String>> claimedDims = config.getDimensionTree();
        if (claimedDims == null || claimedDims.isEmpty()) {
            return;
        }

        dimensions = claimedDims.keySet().stream().map(AttrDimension::new).collect(Collectors.toList());
        Map<String, AttrDimension> dimMap = new HashMap<>();
        for (AttrDimension dim : dimensions) {
            dimMap.put(dim.getName(), dim);
        }
        for (Map.Entry<String, List<String>> entry : claimedDims.entrySet()) {
            AttrDimension parent = dimMap.get(entry.getKey());
            if (entry.getValue() != null) {
                List<AttrDimension> children = entry.getValue().stream().map(dimMap::get).collect(Collectors.toList());
                parent.setChildren(children);
            }
        }

        // verify
        for (String dimField : dimMap.keySet()) {
            FieldMetadata dimFm = null;
            for (FieldMetadata fm : source.getSchema()) {
                if (fm.getFieldName().equals(dimField)) {
                    dimFm = fm;
                    break;
                }
            }
            if (dimFm == null) {
                throw new IllegalArgumentException("Cannot find dimension attribute " + dimField);
            }
            if (!dimFm.getJavaType().equals(String.class)) {
                throw new IllegalArgumentException("Only support string type dimension fields. Dimension " + dimField
                        + " is " + dimFm.getJavaType());
            }
        }
    }

    private List<String> getGeneratedDimAttrs() {
        if (dimensions != null && !dimensions.isEmpty()) {
            return dimensions.stream().map(d -> DIM_PREFIX + d.getName()).collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private List<String> getGeneratedDedupAttrs() {
        if (dedupFields != null && !dedupFields.isEmpty()) {
            return dedupFields.stream().map(d -> DEDUP_PREFIX + d).collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public Class<CalculateStatsConfig> getTransformerConfigClass() {
        return CalculateStatsConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

}
