package com.latticeengines.domain.exposed.util;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_ALGO;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_BKTS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_COUNT;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_NAME;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.BooleanBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.DiscreteBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.IntervalBucket;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.statistics.CategoryStatistics;
import com.latticeengines.domain.exposed.metadata.statistics.CategoryTopNTree;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.metadata.statistics.SubcategoryStatistics;
import com.latticeengines.domain.exposed.metadata.statistics.TopAttribute;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.metadata.transaction.NamedPeriod;
import com.latticeengines.domain.exposed.metadata.transaction.TransactionMetrics;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;

public class StatsCubeUtils {

    private static final Logger log = LoggerFactory.getLogger(StatsCubeUtils.class);
    private static final List<String> TOP_FIRMOGRAPHIC_ATTRS = Arrays.asList( //
            DataCloudConstants.ATTR_IS_PRIMARY_LOCATION, //
            DataCloudConstants.ATTR_COUNTRY, //
            DataCloudConstants.ATTR_REV_RANGE, //
            DataCloudConstants.ATTR_NUM_EMP_RANGE, //
            DataCloudConstants.ATTR_LDC_INDUSTRY //
    );
    static final String HASEVER_PURCHASED_SUFFIX = String.format("%s_%s", NamedPeriod.HASEVER.getName(), "Purchased");

    public static StatsCube parseAvro(Iterator<GenericRecord> records) {
        final AtomicLong maxCount = new AtomicLong(0L);
        Map<String, AttributeStats> statsMap = new HashMap<>();
        records.forEachRemaining(record -> {
            AttributeStats attrStats = parseAttrStats(record);
            statsMap.put(record.get(STATS_ATTR_NAME).toString(), attrStats);
            synchronized (maxCount) {
                maxCount.set(Math.max(maxCount.get(), attrStats.getNonNullCount()));
            }
        });
        StatsCube cube = new StatsCube();
        cube.setStatistics(statsMap);
        cube.setCount(maxCount.get());
        return cube;
    }

    private static AttributeStats parseAttrStats(GenericRecord record) {
        AttributeStats stats = new AttributeStats();

        long attrCount = (long) record.get(STATS_ATTR_COUNT);
        stats.setNonNullCount(attrCount);

        if (record.get(STATS_ATTR_ALGO) != null) {
            String bktSerialized = record.get(STATS_ATTR_BKTS) == null ? "" : record.get(STATS_ATTR_BKTS).toString();
            Map<Integer, Long> bktCounts = new HashMap<>();
            if (StringUtils.isNotBlank(bktSerialized)) {
                String[] tokens = bktSerialized.split("\\|");
                for (String token : tokens) {
                    String[] parts = token.split(":");
                    int bktId = Integer.valueOf(parts[0]);
                    long bktCnt = Long.valueOf(parts[1]);
                    bktCounts.put(bktId, bktCnt);
                }
            }
            String bktAlgoSerialized = record.get(STATS_ATTR_ALGO).toString();
            BucketAlgorithm algorithm = JsonUtils.deserialize(bktAlgoSerialized, BucketAlgorithm.class);
            Buckets buckets = parseBuckets(algorithm, bktCounts);

            stats.setBuckets(buckets);
        }

        return stats;
    }

    private static Buckets parseBuckets(BucketAlgorithm algorithm, Map<Integer, Long> bktCounts) {
        Buckets buckets = new Buckets();
        buckets.setType(algorithm.getBucketType());
        List<Bucket> bucketList = initializeBucketList(algorithm);
        for (int i = 0; i < bucketList.size(); i++) {
            int bktId = i + 1;
            if (!bktCounts.containsKey(bktId)) {
                bktCounts.put(bktId, 0L);
            }
        }
        bktCounts.forEach((bktId, bktCnt) -> updateBucket(bucketList.get(bktId - 1), algorithm, bktId, bktCnt));
        if (bucketList.isEmpty()) {
            return null;
        } else {
            buckets.setBucketList(bucketList);
            return buckets;
        }
    }

    private static List<Bucket> initializeBucketList(BucketAlgorithm algorithm) {
        List<Bucket> bucketList = new ArrayList<>();
        List<String> labels = algorithm.generateLabels();
        for (int i = 1; i < labels.size(); i++) {
            String label = labels.get(i);
            Bucket bucket = new Bucket();
            bucket.setId((long) i);
            bucket.setLabel(label);
            bucket.setCount(0L);
            bucketList.add(bucket);
        }
        return bucketList;
    }

    private static Bucket updateBucket(Bucket bucket, BucketAlgorithm algorithm, int bktId, long bktCnt) {
        bucket.setCount(bktCnt);

        if (algorithm instanceof BooleanBucket) {
            updateBooleanBucket(bucket, (BooleanBucket) algorithm, bktId);
        } else if (algorithm instanceof IntervalBucket) {
            updateIntervalBucket(bucket, (IntervalBucket) algorithm, bktId);
        } else if (algorithm instanceof CategoricalBucket) {
            updateCategoricalBucket(bucket, (CategoricalBucket) algorithm, bktId);
        } else if (algorithm instanceof DiscreteBucket) {
            updateDiscreteBucket(bucket, (DiscreteBucket) algorithm, bktId);
        } else {
            throw new UnsupportedOperationException(
                    "Do not know how to parse algorithm of type " + algorithm.getClass());
        }

        return bucket;
    }

    private static void updateBooleanBucket(Bucket bucket, BooleanBucket algo, int bktId) {
        String val = null;
        switch (bktId) {
        case 1:
            val = algo.getTrueLabelWithDefault();
            break;
        case 2:
            val = algo.getFalseLabelWithDefault();
            break;
        default:
        }
        bucket.setLabel(val);
        bucket.setValues(Collections.singletonList(val));
        bucket.setComparisonType(ComparisonType.EQUAL);
    }

    private static void updateIntervalBucket(Bucket bucket, IntervalBucket algo, int bktId) {
        List<Number> boundaries = algo.getBoundaries();
        Number min = bktId == 1 ? null : boundaries.get(bktId - 2);
        Number max = bktId == boundaries.size() + 1 ? null : boundaries.get(bktId - 1);
        List<String> labels = algo.generateLabels();
        String bucketLabel = labels.get(bktId);
        bucket.setLabel(bucketLabel);
        if (min != null && max != null) {
            bucket.setComparisonType(ComparisonType.GTE_AND_LT);
            bucket.setValues(Arrays.asList(min, max));
        } else if (min != null) {
            bucket.setComparisonType(ComparisonType.GREATER_OR_EQUAL);
            bucket.setValues(Collections.singletonList(min));
        } else if (max != null) {
            bucket.setComparisonType(ComparisonType.LESS_THAN);
            bucket.setValues(Collections.singletonList(max));
        } else {
            throw new IllegalArgumentException("A bucket cannot have both min and max being null");
        }
    }

    private static void updateCategoricalBucket(Bucket bucket, CategoricalBucket algo, int bktId) {
        List<String> labels = algo.generateLabels();
        String bucketLabel = labels.get(bktId);
        bucket.setLabel(bucketLabel);
        bucket.setValues(Collections.singletonList(bucketLabel));
        bucket.setComparisonType(ComparisonType.EQUAL);
    }

    private static void updateDiscreteBucket(Bucket bucket, DiscreteBucket algo, int bktId) {
        List<String> labels = algo.generateLabels();
        String bucketLabel = labels.get(bktId);
        bucket.setLabel(bucketLabel);
        bucket.setValues(Collections.singletonList(bucketLabel));
        bucket.setComparisonType(ComparisonType.EQUAL);
    }

    public static Statistics constructStatistics(Map<BusinessEntity, StatsCube> cubeMap,
            Map<BusinessEntity, List<ColumnMetadata>> cmMap) {
        Statistics statistics = new Statistics();

        for (Map.Entry<BusinessEntity, StatsCube> cubeEntry : cubeMap.entrySet()) {
            BusinessEntity entity = cubeEntry.getKey();
            StatsCube cube = cubeEntry.getValue();
            if (cmMap.containsKey(entity)) {
                List<ColumnMetadata> cmList = cmMap.get(entity);
                addStats(entity, cube, cmList, statistics);
            } else {
                log.warn("Did not provide column metadata for entity " + entity //
                        + ", skipping the stats for the whole entity.");
            }
        }

        return statistics;
    }

    private static void addStats(BusinessEntity entity, StatsCube cube, List<ColumnMetadata> cmList,
            Statistics statistics) {
        Map<String, ColumnMetadata> cmMap = new HashMap<>();
        cmList.forEach(cm -> cmMap.put(cm.getColumnId(), cm));
        Map<String, AttributeStats> attrStatsMap = cube.getStatistics();
        for (String name : attrStatsMap.keySet()) {
            ColumnMetadata cm = cmMap.get(name);
            if (cm == null) {
                log.warn("Cannot find attribute " + name + " in the provided column metadata for " + entity
                        + ", skipping it.");
                continue;
            }
            AttributeStats statsInCube = attrStatsMap.get(name);
            if (BusinessEntity.PurchaseHistory.equals(entity)) {
                statsInCube = convertPurchaseHistoryStats(name, statsInCube);
                if (statsInCube == null) {
                    log.warn("No valid transaction bucket left for " + name + ", skipping it");
                    continue;
                }
            }

            AttributeLookup attrLookup = new AttributeLookup(entity, name);
            Category category = cm.getCategory() == null ? Category.DEFAULT : cm.getCategory();
            String subCategory = cm.getSubcategory() == null ? "Other" : cm.getSubcategory();
            // create map entries if not there
            if (!statistics.hasCategory(category)) {
                statistics.putCategory(category, new CategoryStatistics());
            }
            CategoryStatistics categoryStatistics = statistics.getCategory(category);
            if (!categoryStatistics.hasSubcategory(subCategory)) {
                categoryStatistics.putSubcategory(subCategory, new SubcategoryStatistics());
            }
            // update the corresponding map entry
            SubcategoryStatistics subcategoryStatistics = statistics.getCategory(category).getSubcategory(subCategory);
            subcategoryStatistics.putAttrStats(attrLookup, statsInCube);
        }
    }

    private static AttributeStats convertPurchaseHistoryStats(String attrName, AttributeStats attrStats) {
        if (!attrName.startsWith("PH_")) {
            return attrStats;
        }

        String productId = TransactionMetrics.getProductIdFromAttr(attrName);
        NamedPeriod namedPeriod = NamedPeriod.fromName(TransactionMetrics.getPeriodFromAttr(attrName));
        TransactionMetrics metric = TransactionMetrics.fromName(TransactionMetrics.getMetricFromAttr(attrName));

        Buckets buckets = attrStats.getBuckets();
        buckets.setType(BucketType.TimeSeries);
        List<Bucket> bucketList = new ArrayList<>();
        buckets.getBucketList().forEach(bucket -> {
            Bucket bucket1 = convertTxnBucket(productId, namedPeriod, metric, bucket);
            if (bucket1 != null) {
                bucketList.add(bucket1);
            }
        });
        if (bucketList.isEmpty()) {
            return null;
        }
        buckets.setBucketList(bucketList);
        attrStats.setBuckets(buckets);
        return attrStats;
    }

    private static Bucket convertTxnBucket(String productId, NamedPeriod namedPeriod, TransactionMetrics metric,
            Bucket bucket) {
        TimeFilter timeFilter = null;
        AggregationFilter spentFilter = null, unitFilter = null;

        String period = null;
        ComparisonType comparator = null;
        List<Object> values = null;
        switch (namedPeriod) {
        case HASEVER:
            timeFilter = TimeFilter.ever();
            break;
        case LASTQUARTER:
            period = TimeFilter.Period.Quarter.name();
            comparator = ComparisonType.EQUAL;
            values = Collections.singletonList(1);
            timeFilter = new TimeFilter(comparator, period, values);
            break;
        default:
            break;
        }

        Boolean negate = null;
        boolean isAmount = false;
        switch (metric) {
        case PURCHASED:
            negate = "No".equalsIgnoreCase(bucket.getLabel());
            break;
        case AMOUNT:
            isAmount = true;
        case QUANTITY:
            AggregationFilter aggFilter = new AggregationFilter(null, null, bucket.getComparisonType(),
                    bucket.getValues());
            if (isAmount) {
                spentFilter = aggFilter;
            } else {
                unitFilter = aggFilter;
            }
            break;
        default:
            break;
        }

        Bucket.Transaction transaction = new Bucket.Transaction(productId, timeFilter, spentFilter, unitFilter, negate);
        bucket.setComparisonType(null);
        bucket.setValues(null);
        bucket.setTransaction(transaction);
        return bucket;
    }

    public static StatsCube toStatsCube(Statistics statistics) {
        StatsCube cube = new StatsCube();
        Map<String, AttributeStats> stats = new HashMap<>();
        for (CategoryStatistics catStats : statistics.getCategories().values()) {
            for (SubcategoryStatistics subCatStats : catStats.getSubcategories().values()) {
                for (Map.Entry<AttributeLookup, AttributeStats> entry : subCatStats.getAttributes().entrySet()) {
                    stats.put(entry.getKey().getAttribute(), retainTop5Bkts(entry.getValue()));
                }
            }
        }
        cube.setStatistics(stats);
        return cube;
    }

    public static Map<BusinessEntity, StatsCube> toStatsCubes(Statistics statistics) {
        Map<BusinessEntity, Map<String, AttributeStats>> statsMap = new HashMap<>();
        for (CategoryStatistics catStats : statistics.getCategories().values()) {
            for (SubcategoryStatistics subCatStats : catStats.getSubcategories().values()) {
                for (Map.Entry<AttributeLookup, AttributeStats> entry : subCatStats.getAttributes().entrySet()) {
                    BusinessEntity entity = entry.getKey().getEntity();
                    if (!statsMap.containsKey(entity)) {
                        statsMap.put(entity, new HashMap<>());
                    }
                    statsMap.get(entity).put(entry.getKey().getAttribute(), retainTop5Bkts(entry.getValue()));
                }
            }
        }
        Map<BusinessEntity, StatsCube> cubes = new HashMap<>();
        statsMap.forEach((entity, stats) -> {
            StatsCube cube = new StatsCube();
            cube.setStatistics(stats);
            cubes.put(entity, cube);
        });
        return cubes;
    }

    private static AttributeStats retainTop5Bkts(AttributeStats attributeStats) {
        if (attributeStats.getBuckets() != null && attributeStats.getBuckets().getBucketList() != null) {
            Buckets buckets = attributeStats.getBuckets();
            List<Bucket> top5Bkts = buckets.getBucketList().stream() //
                    .filter(bkt -> bkt.getCount() != null && bkt.getCount() > 0) //
                    .sorted(Comparator.comparing(bkt -> -bkt.getCount())) //
                    .limit(5) //
                    .collect(Collectors.toList());
            if (attributeStats.getBuckets().getBucketList().size() > top5Bkts.size()) {
                buckets.setHasMore(true);
            }
            buckets.setBucketList(top5Bkts);
        }
        return attributeStats;
    }

    public static TopNTree toTopNTree(Statistics statistics, boolean includeTopBkt, List<ColumnMetadata> cms) {
        TopNTree topNTree = new TopNTree();
        Map<Category, CategoryTopNTree> catTrees = new HashMap<>();
        for (Map.Entry<Category, CategoryStatistics> entry : statistics.getCategories().entrySet()) {
            catTrees.put(entry.getKey(),
                    toCatTopTree(entry.getKey(), entry.getValue(), includeTopBkt, getAttrsToHide(cms)));
        }
        topNTree.setCategories(catTrees);
        return topNTree;
    }

    private static Map<Category, Set<String>> getAttrsToHide(List<ColumnMetadata> cms) {
        Map<Category, Set<String>> attrsToHide = new HashMap<>();
        if (cms == null) {
            return attrsToHide;
        }
        for (ColumnMetadata cm : cms) {
            if (cm.getFundamentalType() == FundamentalType.DATE || cm.getLogicalDataType() == LogicalDataType.Date) {
                if (!attrsToHide.containsKey(cm.getCategory())) {
                    attrsToHide.put(cm.getCategory(), new HashSet<>());
                }
                attrsToHide.get(cm.getCategory()).add(cm.getColumnId());
            }
        }
        return attrsToHide;
    }

    private static CategoryTopNTree toCatTopTree(Category category, CategoryStatistics catStats, boolean includeTopBkt,
            Map<Category, Set<String>> attrsToHide) {
        CategoryTopNTree topNTree = new CategoryTopNTree();
        Map<String, List<TopAttribute>> subCatTrees = new HashMap<>();
        for (Map.Entry<String, SubcategoryStatistics> entry : catStats.getSubcategories().entrySet()) {
            subCatTrees.put(entry.getKey(), toSubcatTopTree(category, entry.getValue(), includeTopBkt, attrsToHide));
        }
        topNTree.setSubcategories(subCatTrees);
        return topNTree;
    }

    private static List<TopAttribute> toSubcatTopTree(Category category, SubcategoryStatistics catStats,
            boolean includeTopBkt, Map<Category, Set<String>> attrsToHide) {
        Comparator<Map.Entry<AttributeLookup, AttributeStats>> comparator = getAttrComparatorForCategory(category);
        return catStats.getAttributes().entrySet().stream() //
                .sorted(comparator) //
                .map(entry -> toTopAttr(category, entry, includeTopBkt)) //
                .filter(attr -> !(attrsToHide.containsKey(category)
                        && attrsToHide.get(category).contains(attr.getAttribute())))
                .collect(Collectors.toList());
    }

    private static TopAttribute toTopAttr(Category category, Map.Entry<AttributeLookup, AttributeStats> entry, boolean includeTopBkt) {
        AttributeStats stats = entry.getValue();
        TopAttribute topAttribute = new TopAttribute(entry.getKey(), stats.getNonNullCount());
        if (includeTopBkt && stats.getBuckets() != null) {
            Comparator<Bucket> comparator = getBktComparatorForCategory(category);
            Bucket topBkt = getTopBkt(stats, comparator);
            if (topBkt != null) {
                topAttribute.setTopBkt(topBkt);
            }
        }
        return topAttribute;
    }

    private static Bucket getTopBkt(AttributeStats attributeStats, Comparator<Bucket> comparator) {
        if (attributeStats.getBuckets() != null) {
            return attributeStats.getBuckets().getBucketList().stream() //
                    .filter(bkt -> bkt.getCount() != null && bkt.getCount() > 0) //
                    .sorted(comparator) //
                    .findFirst().orElse(null);
        } else {
            return null;
        }
    }

    static Comparator<Bucket> getBktComparatorForCategory(Category category) {
        switch (category) {
            case INTENT:
                return intentBktComparator();
            case WEBSITE_PROFILE:
            case TECHNOLOGY_PROFILE:
                return techBktComparator();
            case PRODUCT_SPEND:
                return productBktComparator();
            default:
                return Comparator.comparing(Bucket::getCount).reversed();
        }
    }

    private static Comparator<Bucket> intentBktComparator() {
        return Comparator.comparing(Bucket::getId).reversed();
    }

    private static Comparator<Bucket> techBktComparator() {
        return Comparator.comparing(Bucket::getId);
    }

    private static Comparator<Bucket> productBktComparator() {
        return (o1, o2) -> {
            if (isBooleanBkt(o1) || isBooleanBkt(o2)) {
                return Comparator.comparing(Bucket::getId).compare(o1, o2);
            } else {
                return Comparator.comparing(Bucket::getCount).reversed().compare(o1, o2);
            }
        };
    }

    private static boolean isBooleanBkt(Bucket bkt) {
        return bkt != null && bkt.getLabel().equalsIgnoreCase("Yes") || bkt.getLabel().equalsIgnoreCase("No");
    }

    private static Comparator<Map.Entry<AttributeLookup, AttributeStats>> defaultAttrComparator() {
        return Comparator.comparing(entry -> -entry.getValue().getNonNullCount());
    }

    private static Comparator<Map.Entry<AttributeLookup, AttributeStats>> getAttrComparatorForCategory(Category category) {
        switch (category) {
            case FIRMOGRAPHICS:
                return firmographicAttrComparator();
            case INTENT:
                return intentAttrComparator();
            case WEBSITE_PROFILE:
            case TECHNOLOGY_PROFILE:
                return techAttrComparator();
            case PRODUCT_SPEND:
                return productAttrComparator();
            default:
                return defaultAttrComparator();
        }
    }

    private static Comparator<Map.Entry<AttributeLookup, AttributeStats>> firmographicAttrComparator() {
        return (o1, o2) -> {
            String attr1 = o1.getKey().getAttribute();
            String attr2 = o2.getKey().getAttribute();
            int topIdx1 = TOP_FIRMOGRAPHIC_ATTRS.indexOf(attr1);
            int topIdx2 = TOP_FIRMOGRAPHIC_ATTRS.indexOf(attr2);
            if (topIdx1 == topIdx2) {
                return attr1.compareTo(attr2);
            } else {
                return topIdx2 - topIdx1;
            }
        };
    }

    private static Comparator<Map.Entry<AttributeLookup, AttributeStats>> intentAttrComparator() {
        Comparator<Bucket> comparator = intentBktComparator();
        return (o1, o2) -> {
            Bucket topBkt1 = getTopBkt(o1.getValue(), comparator);
            Bucket topBkt2 = getTopBkt(o2.getValue(), comparator);
            Integer bktId1 = topBkt1 != null ? topBkt1.getId().intValue() : 0;
            Integer bktId2 = topBkt2 != null ? topBkt2.getId().intValue() : 0;
            if (bktId1.equals(bktId2)) {
                Long count1 = topBkt1 != null ? topBkt1.getCount() : 0;
                Long count2 = topBkt2 != null ? topBkt2.getCount() : 0;
                if (count1.equals(count2)) {
                    String attr1 = o1.getKey().getAttribute();
                    String attr2 = o2.getKey().getAttribute();
                    return attr1.compareTo(attr2);
                } else {
                    return count2.compareTo(count1);
                }
            } else {
                return bktId2.compareTo(bktId1);
            }
        };
    }

    private static Comparator<Map.Entry<AttributeLookup, AttributeStats>> techAttrComparator() {
        Comparator<Bucket> comparator = techBktComparator();
        return (o1, o2) -> {
            Bucket topBkt1 = getTopBkt(o1.getValue(), comparator);
            Bucket topBkt2 = getTopBkt(o2.getValue(), comparator);
            Integer bktId1 = topBkt1 != null ? topBkt1.getId().intValue() : Integer.MAX_VALUE;
            Integer bktId2 = topBkt2 != null ? topBkt2.getId().intValue() : Integer.MAX_VALUE;
            if (bktId1.equals(bktId2)) {
                Long count1 = topBkt1 != null ? topBkt1.getCount() : 0;
                Long count2 = topBkt2 != null ? topBkt2.getCount() : 0;
                if (count1.equals(count2)) {
                    String attr1 = o1.getKey().getAttribute();
                    String attr2 = o2.getKey().getAttribute();
                    return attr1.compareTo(attr2);
                } else {
                    return count2.compareTo(count1);
                }
            } else {
                return bktId1.compareTo(bktId2);
            }
        };
    }

    private static Comparator<Map.Entry<AttributeLookup, AttributeStats>> productAttrComparator() {
        return (o1, o2) -> {
            String attr1 = o1.getKey().getAttribute();
            String attr2 = o2.getKey().getAttribute();
            int rank1 = productAttrSuffixRank(attr1);
            int rank2 = productAttrSuffixRank(attr2);
            if (rank1 == rank2) {
                return attr1.compareTo(attr2);
            } else {
                return rank1 - rank2;
            }
        };
    }

    private static int productAttrSuffixRank(String attr) {
        if (attr.endsWith(HASEVER_PURCHASED_SUFFIX)) {
            return  0;
        }
        if (attr.endsWith("LastQuarter_Amount")) {
            return 1;
        }
        if (attr.endsWith("LastQuarter_Quantity")) {
            return 2;
        }
        else {
            return Integer.MAX_VALUE;
        }
    }

}
