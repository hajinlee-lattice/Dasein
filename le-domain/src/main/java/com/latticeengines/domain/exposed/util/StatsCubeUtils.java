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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.BooleanBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.DateBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.DiscreteBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.IntervalBucket;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.metadata.statistics.CategoryStatistics;
import com.latticeengines.domain.exposed.metadata.statistics.CategoryTopNTree;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.metadata.statistics.SubcategoryStatistics;
import com.latticeengines.domain.exposed.metadata.statistics.TopAttribute;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.metadata.transaction.NamedPeriod;
import com.latticeengines.domain.exposed.metadata.transaction.TransactionMetrics;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@SuppressWarnings("deprecation")
public class StatsCubeUtils {

    static final String HASEVER_PURCHASED_SUFFIX = String.format("%s_%s", NamedPeriod.HASEVER.getName(), "Purchased");
    private static final Logger log = LoggerFactory.getLogger(StatsCubeUtils.class);
    private static final List<String> TOP_FIRMOGRAPHIC_ATTRS = Lists.reverse(Arrays.asList( //
            DataCloudConstants.ATTR_LDC_INDUSTRY, //
            DataCloudConstants.ATTR_REV_RANGE, //
            DataCloudConstants.ATTR_NUM_EMP_RANGE, //
            DataCloudConstants.ATTR_LDC_DOMAIN, //
            DataCloudConstants.ATTR_LE_NUMBER_OF_LOCATIONS, //
            DataCloudConstants.ATTR_COUNTRY, //
            DataCloudConstants.ATTR_CITY, //
            DataCloudConstants.ATTR_STATE));
    private static final ConcurrentMap<BusinessEntity, Set<String>> SYSTEM_ATTRS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<BusinessEntity, Set<String>> SYSTEM_ATTRS_ENTITY_MATCH_ENABLED = new ConcurrentHashMap<>();

    private static final ConcurrentMap<BusinessEntity, Set<String>> INTERNAL_LOOKUPID_ATTRS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<BusinessEntity, Set<String>> INTERNAL_LOOKUPID_ATTRS_ENTITY_MATCH_ENABLED = new ConcurrentHashMap<>();

    private static final Scheduler SORTER = Schedulers.newParallel("attr-sorter");

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
            return buckets;
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
        } else if (algorithm instanceof DiscreteBucket) {
            updateDiscreteBucket(bucket, (DiscreteBucket) algorithm, bktId);
        } else if (algorithm instanceof CategoricalBucket) {
            updateCategoricalBucket(bucket, (CategoricalBucket) algorithm, bktId);
        } else if (algorithm instanceof DateBucket) {
            updateDateBucket(bucket, (DateBucket) algorithm, bktId);
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

    private static void updateDiscreteBucket(Bucket bucket, DiscreteBucket algo, int bktId) {
        List<String> labels = algo.generateLabels();
        String bucketLabel = labels.get(bktId);
        bucket.setLabel(bucketLabel);
        bucket.setValues(Collections.singletonList(bucketLabel));
        bucket.setComparisonType(ComparisonType.EQUAL);
    }

    private static void updateCategoricalBucket(Bucket bucket, CategoricalBucket algo, int bktId) {
        List<String> labels = algo.generateLabels();
        String bucketLabel = labels.get(bktId);
        bucket.setLabel(bucketLabel);
        bucket.setValues(Collections.singletonList(bucketLabel));
        bucket.setComparisonType(ComparisonType.EQUAL);
    }

    private static void updateDateBucket(Bucket bucket, DateBucket algo, int bktId) {
        List<String> labels = algo.generateLabels();
        String bucketLabel = labels.get(bktId);
        bucket.setLabel(bucketLabel);
        List<Integer> dayBoundaries = algo.getDayBoundaries();
        Integer dayBound = bktId == dayBoundaries.size() + 1 ? null : dayBoundaries.get(bktId - 1);
        if (dayBound != null) {
            bucket.setDateFilter(TimeFilter.last(dayBound, PeriodStrategy.Template.Day.name()));
        } else {
            bucket.setDateFilter(TimeFilter.ever(PeriodStrategy.Template.Day.name()));
        }

    }

    public static void sortRatingBuckets(AttributeStats attrStats) {
        Buckets buckets = attrStats.getBuckets();
        if (buckets != null && BucketType.Enum.equals(buckets.getType())
                && CollectionUtils.isNotEmpty(buckets.getBucketList())) {
            List<Bucket> bucketList = buckets.getBucketList().stream().sorted(Comparator.comparing(Bucket::getLabel))
                    .collect(Collectors.toList());
            buckets.setBucketList(bucketList);
        }
    }

    public static void addLift(AttributeStats attrStats, Map<String, Double> lift) {
        Buckets buckets = attrStats.getBuckets();
        if (buckets != null && CollectionUtils.isNotEmpty(buckets.getBucketList())) {
            buckets.getBucketList().forEach(bkt -> {
                String label = bkt.getLabel();
                if (lift.containsKey(label)) {
                    bkt.setLift(lift.get(label));
                }
            });
        }
    }

    public static AttributeStats convertPurchaseHistoryStats(String attrName, AttributeStats attrStats) {
        if (ActivityMetricsUtils.isHasPurchasedAttr(attrName)) {
            return convertHasPurchasedStats(attrName, attrStats);
        }

        if (ActivityMetricsUtils.isSpendChangeAttr(attrName)) {
            return convertSpendChangeStats(attrName, attrStats);
        }

        return attrStats;
    }

    private static AttributeStats convertHasPurchasedStats(String attrName, AttributeStats attrStats) {
        String productId = ActivityMetricsUtils.getProductIdFromFullName(attrName);

        Buckets buckets = attrStats.getBuckets();
        buckets.setType(BucketType.TimeSeries);
        List<Bucket> bucketList = new ArrayList<>();
        buckets.getBucketList().forEach(bucket -> {
            Bucket bucket1 = convertTxnBucketForHasPurchased(productId, bucket);
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

    private static Bucket convertTxnBucketForHasPurchased(String productId, Bucket bucket) {
        Bucket.Transaction transaction = new Bucket.Transaction(productId, TimeFilter.ever(), null, null,
                "No".equalsIgnoreCase(bucket.getLabel()));
        bucket.setComparisonType(null);
        bucket.setValues(null);
        bucket.setTransaction(transaction);
        return bucket;
    }

    private static AttributeStats convertSpendChangeStats(String attrName, AttributeStats attrStats) {
        Buckets buckets = attrStats.getBuckets();
        buckets.setType(BucketType.PercentChange);
        List<Bucket> bucketList = new ArrayList<>();
        buckets.getBucketList().forEach(bucket -> {
            Bucket bucket1 = convertBucketToChgBucket(bucket);
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

    public static Bucket convertBucketToChgBucket(Bucket bucket) {
        if (CollectionUtils.isEmpty(bucket.getValues())) {
            return bucket;
        }
        Bucket.Change chg = new Bucket.Change();
        List<Object> absVals = new ArrayList<>();
        switch (bucket.getComparisonType()) {
            case LESS_THAN:
                Double val = valObjToDouble(bucket.getValues().get(0));
                if (val <= 0) {
                    chg.setDirection(Bucket.Change.Direction.DEC);
                    chg.setComparisonType(Bucket.Change.ComparisonType.AT_LEAST);
                } else {
                    chg.setDirection(Bucket.Change.Direction.INC);
                    chg.setComparisonType(Bucket.Change.ComparisonType.AS_MUCH_AS);
                }
                absVals.add(Math.abs(val));
                break;
            case GREATER_OR_EQUAL:
                val = valObjToDouble(bucket.getValues().get(0));
                if (val < 0) {
                    chg.setDirection(Bucket.Change.Direction.DEC);
                    chg.setComparisonType(Bucket.Change.ComparisonType.AS_MUCH_AS);
                } else {
                    chg.setDirection(Bucket.Change.Direction.INC);
                    chg.setComparisonType(Bucket.Change.ComparisonType.AT_LEAST);
                }
                absVals.add(Math.abs(val));
                break;
            case EQUAL:
                val = valObjToDouble(bucket.getValues().get(0));
                if (val < 0) {
                    chg.setDirection(Bucket.Change.Direction.DEC);
                } else {
                    chg.setDirection(Bucket.Change.Direction.INC);
                }
                chg.setComparisonType(Bucket.Change.ComparisonType.BETWEEN);
                absVals.add(Math.abs(val));
                absVals.add(Math.abs(val));
                break;
            case GTE_AND_LT:
                Double val1 = valObjToDouble(bucket.getValues().get(0));
                Double val2 = valObjToDouble(bucket.getValues().get(1));
                if (val2 > 0) {
                    chg.setDirection(Bucket.Change.Direction.INC);
                    absVals.add(Math.abs(val1));
                    absVals.add(Math.abs(val2));
                } else {
                    chg.setDirection(Bucket.Change.Direction.DEC);
                    absVals.add(Math.abs(val2));
                    absVals.add(Math.abs(val1));
                }
                chg.setComparisonType(Bucket.Change.ComparisonType.BETWEEN);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unknown comparison type in bucket: " + bucket.getComparisonType());
        }

        chg.setAbsVals(absVals);
        bucket.setChange(chg);
        bucket.setComparisonType(null);
        bucket.setValues(null);
        return bucket;
    }

    public static Bucket convertChgBucketToBucket(Bucket bucket) {
        if (bucket.getChange() == null) {
            return bucket;
        }
        Bucket.Change chg = bucket.getChange();
        List<Object> vals = new ArrayList<>();
        switch (chg.getDirection()) {
            case INC:
                switch (chg.getComparisonType()) {
                    case AT_LEAST:
                        vals.add(valObjToDouble(chg.getAbsVals().get(0)));
                        bucket.setComparisonType(ComparisonType.GREATER_OR_EQUAL);
                        break;
                    case AS_MUCH_AS:
                        vals.add(0.0D);
                        vals.add(valObjToDouble(chg.getAbsVals().get(0)));
                        bucket.setComparisonType(ComparisonType.GTE_AND_LTE);
                        break;
                    case BETWEEN:
                        vals.add(valObjToDouble(chg.getAbsVals().get(0)));
                        vals.add(valObjToDouble(chg.getAbsVals().get(1)));
                        bucket.setComparisonType(ComparisonType.GTE_AND_LTE);
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unknown comparison type in Bucket.Change: " + chg.getComparisonType());
                }
                break;
            case DEC:
                switch (chg.getComparisonType()) {
                    case AT_LEAST:
                        vals.add(valObjToDouble(chg.getAbsVals().get(0), true));
                        bucket.setComparisonType(ComparisonType.LESS_OR_EQUAL);
                        break;
                    case AS_MUCH_AS:
                        vals.add(valObjToDouble(chg.getAbsVals().get(0), true));
                        vals.add(0.0D);
                        bucket.setComparisonType(ComparisonType.GTE_AND_LTE);
                        break;
                    case BETWEEN:
                        vals.add(valObjToDouble(chg.getAbsVals().get(1), true));
                        vals.add(valObjToDouble(chg.getAbsVals().get(0), true));
                        bucket.setComparisonType(ComparisonType.GTE_AND_LTE);
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unknown comparison type in Bucket.Change: " + chg.getComparisonType());
                }
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported direction in Bucket.Change: " + chg.getDirection());
        }
        bucket.setValues(vals);
        bucket.setChange(null);
        return bucket;
    }

    // Numerical bucket gives Number as value type, Discrete bucket gives String
    // as value type
    // Not sure what type is passed in. Cast to String first.
    private static Double valObjToDouble(Object obj) {
        return Double.valueOf(String.valueOf(obj));
    }

    private static Double valObjToDouble(Object obj, boolean neg) {
        Double dbl = valObjToDouble(obj);
        if (neg && dbl != 0.0) {
            dbl = -dbl;
        }
        return dbl;
    }

    public static boolean shouldHideAttr(BusinessEntity entity, ColumnMetadata cm, boolean entityMatchEnabled) {
        // Hide Date Attributes not in category Account Attributes (aka "My Attributes") or Contact Attributes.
        // Also hide all system attributes.
        return (cm.isDateAttribute() && !(Category.ACCOUNT_ATTRIBUTES.equals(cm.getCategory())
                || Category.CONTACT_ATTRIBUTES.equals(cm.getCategory())))
                || isSystemAttribute(entity, cm, entityMatchEnabled);
    }

    public static boolean isDateAttribute(ColumnMetadata cm) {
        return cm.isDateAttribute();
    }

    public static boolean isSystemAttribute(BusinessEntity entity, ColumnMetadata cm, boolean entityMatchEnabled) {
        return getSystemAttrs(entity, entityMatchEnabled).contains(cm.getAttrName());
    }

    private static Set<String> getSystemAttrs(BusinessEntity entity, boolean entityMatchEnabled) {
        ConcurrentMap<BusinessEntity, Set<String>> systemAttrsMap = getSystemAttrsMap(entityMatchEnabled);
        if (!systemAttrsMap.containsKey(entity)) {
            synchronized (StringUtils.class) {
                if (!systemAttrsMap.containsKey(entity)) {
                    Set<String> systemAttrs = new HashSet<>();
                    SchemaRepository.getSystemAttributes(entity, entityMatchEnabled)
                            .forEach(interfaceName -> systemAttrs.add(interfaceName.name()));
                    systemAttrsMap.put(entity, systemAttrs);
                }
            }
        }
        return systemAttrsMap.get(entity);
    }

    private static ConcurrentMap<BusinessEntity, Set<String>> getSystemAttrsMap(boolean entityMatchEnabled) {
        if (entityMatchEnabled) {
            return SYSTEM_ATTRS_ENTITY_MATCH_ENABLED;
        } else {
            return SYSTEM_ATTRS;
        }
    }

    private static ConcurrentMap<BusinessEntity, Set<String>> getInternalLookupIdAttrsMap(
            boolean entityMatchEnabled) {
        if (entityMatchEnabled) {
            return INTERNAL_LOOKUPID_ATTRS_ENTITY_MATCH_ENABLED;
        } else {
            return INTERNAL_LOOKUPID_ATTRS;
        }
    }

    @Deprecated
    public static void processPurchaseHistoryCategory(TopNTree topNTree, Map<String, String> productMap) {
        if (MapUtils.isEmpty(productMap) || !topNTree.hasCategory(Category.PRODUCT_SPEND)) {
            return;
        }
        CategoryTopNTree catTopNTree = topNTree.getCategory(Category.PRODUCT_SPEND);
        Map<String, List<TopAttribute>> subcatMap = new HashMap<>();
        catTopNTree.getSubcategories().values().forEach(attrs -> attrs.forEach(attr -> {
            String prodId = TransactionMetrics.getProductIdFromAttr(attr.getAttribute());
            String prodName = productMap.get(prodId);
            String subcategory = StringUtils.isNotBlank(prodName) ? prodName : "Other";
            if (!subcatMap.containsKey(subcategory)) {
                subcatMap.put(subcategory, new ArrayList<>());
            }
            subcatMap.get(subcategory).add(attr);
        }));
        catTopNTree.setSubcategories(subcatMap);
    }

    public static void sortBkts(StatsCube cube, BusinessEntity entity) {
        if (cube != null && MapUtils.isNotEmpty(cube.getStatistics())) {
            cube.getStatistics().forEach((attrName, attrStats) -> {
                Buckets buckets = attrStats.getBuckets();
                if (buckets != null && CollectionUtils.isNotEmpty(buckets.getBucketList())) {
                    Comparator<Bucket> comparator = getBktComparator(entity, buckets.getType(), attrName);
                    if (comparator != null) {
                        buckets.getBucketList().sort(comparator);
                    }
                }
            });
        }
    }

    private static Comparator<Bucket> getBktComparator(BusinessEntity entity, BucketType bucketType, String attrName) {
        switch (entity) {
            case Rating:
            case PurchaseHistory:
                return null;
            default:
                return defaultBktComparator(bucketType, attrName);
        }
    }

    private static Comparator<Bucket> defaultBktComparator(BucketType bucketType, String attrName) {
        // reverse id ordering for intent
        if (attrName.startsWith("BmbrSurge_") && attrName.endsWith("_Intent")) {
            return Comparator.comparing(Bucket::getId).reversed();
        } else if (BucketType.Enum.equals(bucketType)) { // only resort enum
                                                         // buckets
            return defaultTopBktComparator();
        } else {
            return null;
        }
    }

    public static TopNTree constructTopNTree(Map<String, StatsCube> cubeMap, Map<String, List<ColumnMetadata>> cmMap,
            boolean includeTopBkt, ColumnSelection.Predefined selectedGroup, boolean entityMatchEnabled) {
        TopNTree topNTree = new TopNTree();
        for (Map.Entry<String, StatsCube> cubeEntry : cubeMap.entrySet()) {
            String key = cubeEntry.getKey();
            StatsCube cube = cubeEntry.getValue();
            if (cmMap.containsKey(key)) {
                List<ColumnMetadata> cmList = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(cmMap.get(key))) {
                    cmMap.get(key).forEach(cm -> {
                        if (selectedGroup != null && cm.isEnabledFor(selectedGroup)) {
                            cmList.add(cm);
                        } else {
                            cmList.add(cm);
                        }
                    });
                }
                addToTopNTree(key, cube, cmList, topNTree, includeTopBkt, entityMatchEnabled);
            } else {
                log.warn("Did not provide column metadata for " + key //
                        + ", skipping the stats for the whole cube.");
            }
        }
        return topNTree;
    }

    private static void addToTopNTree(String key, StatsCube cube, List<ColumnMetadata> cmList, TopNTree topNTree,
            boolean includeTopBkt, boolean entityMatchEnabled) {
        BusinessEntity entity = BusinessEntity.valueOf(key);
        Map<String, ColumnMetadata> cmMap = new HashMap<>();
        cmList.forEach(cm -> cmMap.put(cm.getAttrName(), cm));
        Map<String, AttributeStats> attrStatsMap = cube.getStatistics();
        for (String name : attrStatsMap.keySet()) {
            ColumnMetadata cm = cmMap.get(name);
            if (cm == null) {
                // log.warn("Cannot find attribute " + name + " in the provided
                // column metadata for " + entity + ", skipping it.");
                continue;
            }
            if (shouldHideAttr(entity, cm, entityMatchEnabled)) {
                continue;
            }
            AttributeStats statsInCube = attrStatsMap.get(name);
            if (statsInCube.getNonNullCount() == 0) {
                // hide if count == 0
                continue;
            }
            Category category = cm.getCategory() == null ? Category.DEFAULT : cm.getCategory();
            String subCategory = cm.getSubcategory() == null ? "Other" : cm.getSubcategory();
            // create map entries if not there
            if (!topNTree.hasCategory(category)) {
                topNTree.putCategory(category, new CategoryTopNTree());
            }
            CategoryTopNTree categoryTopNTree = topNTree.getCategory(category);
            if (!categoryTopNTree.hasSubcategory(subCategory)) {
                categoryTopNTree.putSubcategory(subCategory, new ArrayList<>());
            }
            // update the corresponding map entry
            List<TopAttribute> topAttributes = topNTree.getCategory(category).getSubcategory(subCategory);
            topAttributes.add(toTopAttr(category, entity, name, statsInCube, includeTopBkt));
        }
        if (includeTopBkt) {
            topNTree.getCategories().forEach((category, categoryTopNTree) -> //
            categoryTopNTree.getSubcategories().forEach((subCat, topAttrs) -> {
                Comparator<TopAttribute> comparator = getTopAttrComparatorForCategory(category);
                topAttrs.sort(comparator);
            }));
        }
    }

    private static TopAttribute toTopAttr(Category category, BusinessEntity entity, String attrName,
            AttributeStats attributeStats, boolean includeTopBkt) {
        AttributeLookup attributeLookup = new AttributeLookup(entity, attrName);
        TopAttribute topAttribute = new TopAttribute(attributeLookup, attributeStats.getNonNullCount());
        if (includeTopBkt && attributeStats.getBuckets() != null) {
            Comparator<Bucket> comparator = getTopBktComparatorForCategory(category);
            Bucket topBkt = getTopBkt(attributeStats, comparator);
            if (topBkt != null) {
                topAttribute.setTopBkt(topBkt);
            }
        }
        return topAttribute;
    }

    private static Bucket getTopBkt(AttributeStats attributeStats, Comparator<Bucket> comparator) {
        if (attributeStats.getBuckets() != null
                && CollectionUtils.isNotEmpty(attributeStats.getBuckets().getBucketList())) {
            return attributeStats.getBuckets().getBucketList().stream() //
                    .filter(bkt -> bkt.getCount() != null && bkt.getCount() > 0) //
                    .min(comparator).orElse(null);
        } else {
            return null;
        }
    }

    private static Comparator<Bucket> getTopBktComparatorForCategory(Category category) {
        switch (category) {
            case INTENT:
                return intentTopBktComparator();
            case WEBSITE_PROFILE:
            case TECHNOLOGY_PROFILE:
                return techTopBktComparator();
            case RATING:
                return ratingTopBktComparator();
            case PRODUCT_SPEND:
            default:
                return defaultTopBktComparator();
        }
    }

    private static Comparator<Bucket> intentTopBktComparator() {
        return Comparator.comparing(Bucket::getId).reversed();
    }

    private static Comparator<Bucket> techTopBktComparator() {
        return Comparator.comparing(Bucket::getId);
    }

    private static Comparator<Bucket> ratingTopBktComparator() {
        return Comparator.comparing(Bucket::getId);
    }

    private static Comparator<Bucket> defaultTopBktComparator() {
        return (o1, o2) -> {
            if (isBooleanBkt(o1) && isBooleanBkt(o2)) {
                return Comparator.comparing(Bucket::getId).compare(o1, o2);
            } else if (isBooleanBkt(o1)) {
                return 1;
            } else if (isBooleanBkt(o2)) {
                return -1;
            } else {
                return Comparator.comparing(Bucket::getCount).reversed().compare(o1, o2);
            }
        };
    }

    private static boolean isBooleanBkt(Bucket bkt) {
        return bkt != null && ("yes".equalsIgnoreCase(bkt.getLabel()) || "no".equalsIgnoreCase(bkt.getLabel()));
    }

    public static Flux<ColumnMetadata> filterByStats(Flux<ColumnMetadata> cmFlux, StatsCube statsCube) {
        if (MapUtils.isEmpty(statsCube.getStatistics())) {
            return cmFlux;
        } else {
            return cmFlux.filter(cm -> {
                String attrName = cm.getAttrName();
                AttributeStats attributeStats = statsCube.getStatistics().get(attrName);
                return attributeStats.getNonNullCount() > 0;
            });
        }
    }

    public static Flux<ColumnMetadata> sortByCategory(Flux<ColumnMetadata> cmFlux, StatsCube statsCube) {
        if (MapUtils.isEmpty(statsCube.getStatistics())) {
            return cmFlux;
        }
        ConcurrentMap<Category, ConcurrentLinkedQueue<Pair<TopAttribute, ColumnMetadata>>> topAttrMap = new ConcurrentHashMap<>();
        Flux<Pair<TopAttribute, ColumnMetadata>> topAttributeFlux = cmFlux.map(cm -> {
            String attrName = cm.getAttrName();
            AttributeStats attributeStats = statsCube.getStatistics().get(attrName);
            Category category = cm.getCategory();
            BusinessEntity entity = cm.getEntity();
            if (entity == null) {
                entity = BusinessEntity.Account;
            }
            TopAttribute topAttribute = toTopAttr(category, entity, attrName, attributeStats, true);
            return Pair.of(topAttribute, cm);
        }).parallel().runOn(SORTER).map(pair -> {
            Category category = pair.getRight().getCategory();
            topAttrMap.putIfAbsent(category, new ConcurrentLinkedQueue<>());
            topAttrMap.get(category).add(pair);
            return pair;
        }).sequential();

        // the groupby method in reactor seems buggy
        // it blocks the thread for a long time when sorting a big flux
        // so change to two blocking steps
        topAttributeFlux.count().block();

        if (MapUtils.isNotEmpty(topAttrMap)) {
            return Flux.fromIterable(topAttrMap.keySet()) //
                    .parallel().runOn(SORTER) //
                    .concatMap(category -> {
                        List<Pair<TopAttribute, ColumnMetadata>> pairs = new ArrayList<>(topAttrMap.get(category));
                        Comparator<Pair<TopAttribute, ColumnMetadata>> comparator = getAttrComparatorForCategory(
                                category);
                        pairs.sort(comparator);
                        AtomicInteger ordering = new AtomicInteger(CollectionUtils.size(pairs));
                        return Flux.fromIterable(pairs).map(Pair::getRight).map(cm -> {
                            cm.setImportanceOrdering(ordering.getAndDecrement());
                            return cm;
                        });
                    }).sequential();
        } else {
            return Flux.empty();
        }
    }

    private static Comparator<Pair<TopAttribute, ColumnMetadata>> getAttrComparatorForCategory(Category category) {
        return (o1, o2) -> getTopAttrComparatorForCategory(category).compare(o1.getLeft(), o2.getLeft());
    }

    private static Comparator<TopAttribute> getTopAttrComparatorForCategory(Category category) {
        switch (category) {
            case FIRMOGRAPHICS:
                return firmographicTopAttrComparator();
            case INTENT:
                return intentTopAttrComparator();
            case WEBSITE_PROFILE:
            case TECHNOLOGY_PROFILE:
                return techTopAttrComparator();
            case RATING:
                return ratingTopAttrComparator(techTopAttrComparator(), defaultTopAttrComparator());
            case PRODUCT_SPEND:
            default:
                return defaultTopAttrComparator();
        }
    }

    private static Comparator<TopAttribute> defaultTopAttrComparator() {
        return (o1, o2) -> {
            String attr1 = o1.getAttribute();
            String attr2 = o2.getAttribute();
            Long count1 = (o1.getTopBkt() == null || o1.getTopBkt().getCount() == null) ? 0L
                    : o1.getTopBkt().getCount();
            Long count2 = (o2.getTopBkt() == null || o2.getTopBkt().getCount() == null) ? 0L
                    : o2.getTopBkt().getCount();
            int countCmp = Long.compare(count2, count1);
            if (countCmp == 0) {
                return StringUtils.compare(attr1, attr2);
            } else {
                return countCmp;
            }
        };
    }

    private static Comparator<TopAttribute> firmographicTopAttrComparator() {
        return (o1, o2) -> {
            String attr1 = o1.getAttribute();
            String attr2 = o2.getAttribute();
            int topIdx1 = TOP_FIRMOGRAPHIC_ATTRS.indexOf(attr1);
            int topIdx2 = TOP_FIRMOGRAPHIC_ATTRS.indexOf(attr2);
            if (topIdx1 == topIdx2) {
                return attr1.compareTo(attr2);
            } else {
                return topIdx2 - topIdx1;
            }
        };
    }

    private static Comparator<TopAttribute> intentTopAttrComparator() {
        return topAttrComparatorByIdAnyCnt(0);
    }

    private static Comparator<TopAttribute> techTopAttrComparator() {
        return topAttrComparatorByIdAnyCnt(Integer.MAX_VALUE);
    }

    private static Comparator<TopAttribute> topAttrComparatorByIdAnyCnt(int defaultId) {
        return (o1, o2) -> {
            Bucket topBkt1 = o1.getTopBkt();
            Bucket topBkt2 = o2.getTopBkt();
            Integer bktId1 = topBkt1 != null ? topBkt1.getId().intValue() : defaultId;
            Integer bktId2 = topBkt2 != null ? topBkt2.getId().intValue() : defaultId;
            if (bktId1.equals(bktId2)) {
                Long count1 = topBkt1 != null ? topBkt1.getCount() : 0;
                Long count2 = topBkt2 != null ? topBkt2.getCount() : 0;
                if (count1.equals(count2)) {
                    String attr1 = o1.getAttribute();
                    String attr2 = o2.getAttribute();
                    return attr1.compareTo(attr2);
                } else {
                    return count2.compareTo(count1);
                }
            } else {
                return bktId1.compareTo(bktId2);
            }
        };
    }

    private static Comparator<TopAttribute> ratingTopAttrComparator(Comparator<TopAttribute> ratingCmp,
            Comparator<TopAttribute> otherCmp) {
        return (o1, o2) -> {
            String attr1 = o1.getAttribute();
            String attr2 = o2.getAttribute();
            boolean isRating1 = attr1.startsWith(RatingEngine.RATING_ENGINE_PREFIX)
                    && RatingEngine.toEngineId(attr1).equals(attr1);
            boolean isRating2 = attr2.startsWith(RatingEngine.RATING_ENGINE_PREFIX)
                    && RatingEngine.toEngineId(attr2).equals(attr2);
            if (isRating1 == isRating2) {
                if (isRating1) {
                    return ratingCmp.compare(o1, o2);
                } else {
                    return otherCmp.compare(o1, o2);
                }
            } else if (isRating1) {
                return -1;
            } else {
                return 1;
            }
        };
    }

    public static Map<String, StatsCube> toStatsCubes(Statistics statistics) {
        Map<BusinessEntity, Map<String, AttributeStats>> statsMap = new HashMap<>();
        for (Map.Entry<Category, CategoryStatistics> catStatsEntry : statistics.getCategories().entrySet()) {
            CategoryStatistics catStats = catStatsEntry.getValue();
            for (SubcategoryStatistics subCatStats : catStats.getSubcategories().values()) {
                for (Map.Entry<AttributeLookup, AttributeStats> entry : subCatStats.getAttributes().entrySet()) {
                    BusinessEntity entity = entry.getKey().getEntity();
                    if (!statsMap.containsKey(entity)) {
                        statsMap.put(entity, new HashMap<>());
                    }
                    statsMap.get(entity).put(entry.getKey().getAttribute(), entry.getValue());
                }
            }
        }
        Map<String, StatsCube> cubes = new HashMap<>();
        statsMap.forEach((entity, stats) -> {
            StatsCube cube = new StatsCube();
            cube.setStatistics(stats);
            cubes.put(entity.name(), cube);
        });
        return cubes;
    }

}
