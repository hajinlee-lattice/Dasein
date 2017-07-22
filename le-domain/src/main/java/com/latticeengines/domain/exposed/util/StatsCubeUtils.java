package com.latticeengines.domain.exposed.util;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_ALGO;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_BKTS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_COUNT;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_NAME;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.BooleanBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.IntervalBucket;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.CategoryStatistics;
import com.latticeengines.domain.exposed.metadata.statistics.CategoryTopNTree;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.metadata.statistics.SubcategoryStatistics;
import com.latticeengines.domain.exposed.metadata.statistics.TopAttribute;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class StatsCubeUtils {

    private static final Logger log = LoggerFactory.getLogger(StatsCubeUtils.class);

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
        buckets.setBucketList(bucketList);
        return buckets;
    }

    private static List<Bucket> initializeBucketList(BucketAlgorithm algorithm) {
        List<Bucket> bucketList = new ArrayList<>();
        List<String> labels = algorithm.generateLabels();
        for (int i = 1; i < labels.size(); i++) {
            String label = labels.get(i);
            Bucket bucket = new Bucket();
            bucket.setId((long) i);
            bucket.setLabel(label);
            bucket.setRange(null);
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
        bucket.setRange(null);
    }

    private static void updateIntervalBucket(Bucket bucket, IntervalBucket algo, int bktId) {
        List<Number> boundaries = algo.getBoundaries();
        Number min = bktId == 1 ? null : boundaries.get(bktId - 2);
        Number max = bktId == boundaries.size() + 1 ? null : boundaries.get(bktId - 1);
        List<String> labels = algo.generateLabels();
        String bucketLabel = labels.get(bktId);
        bucket.setLabel(bucketLabel);
        bucket.setRange(Pair.of(min, max));
    }

    private static void updateCategoricalBucket(Bucket bucket, CategoricalBucket algo, int bktId) {
        List<String> labels = algo.generateLabels();
        String bucketLabel = labels.get(bktId);
        bucket.setLabel(bucketLabel);
        bucket.setRange(null);
    }

    public static Statistics constructStatistics(StatsCube statsCube,
            List<Pair<BusinessEntity, List<ColumnMetadata>>> metadataPairs) {
        Statistics statistics = new Statistics();
        Map<String, Pair<BusinessEntity, ColumnMetadata>> cmMap = convertToMap(metadataPairs);

        Map<String, AttributeStats> attrStatsMap = statsCube.getStatistics();
        Map<BusinessEntity, Long> counts = new HashMap<>();
        counts.put(BusinessEntity.Account, statsCube.getCount());
        statistics.setCounts(counts);
        for (String name : attrStatsMap.keySet()) {
            Pair<BusinessEntity, ColumnMetadata> pair = cmMap.get(name);
            if (pair == null) {
                log.warn("Cannot find attribute " + name + " in any of the provided column metadata. Skip it.");
                continue;
            }

            BusinessEntity entity = pair.getLeft();
            ColumnMetadata cm = pair.getRight();
            AttributeLookup attrLookup = new AttributeLookup(entity, name);
            Category category = cm.getCategory() == null ? Category.DEFAULT : cm.getCategory();
            String subCategory = cm.getSubcategory() == null ? "Other" : cm.getSubcategory();

            AttributeStats statsInCube = attrStatsMap.get(name);
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

        return statistics;
    }

    private static Map<String, Pair<BusinessEntity, ColumnMetadata>> convertToMap(
            List<Pair<BusinessEntity, List<ColumnMetadata>>> pairs) {
        Map<String, Pair<BusinessEntity, ColumnMetadata>> map = new HashMap<>();
        for (Pair<BusinessEntity, List<ColumnMetadata>> pair : pairs) {
            BusinessEntity entity = pair.getLeft();
            for (ColumnMetadata cm : pair.getRight()) {
                if (!map.containsKey(cm.getColumnId())) {
                    map.put(cm.getColumnId(), Pair.of(entity, cm));
                }
            }
        }
        return map;
    }

    public static StatsCube toStatsCube(Statistics statistics) {
        StatsCube cube = new StatsCube();
        Map<String, AttributeStats> stats = new HashMap<>();
        for (CategoryStatistics catStats: statistics.getCategories().values()) {
            for (SubcategoryStatistics subCatStats: catStats.getSubcategories().values()) {
                for (Map.Entry<AttributeLookup, AttributeStats> entry: subCatStats.getAttributes().entrySet()) {
                    stats.put(entry.getKey().getAttribute(), entry.getValue());
                }
            }
        }
        cube.setStatistics(stats);
        if (statistics.getCounts().containsKey(BusinessEntity.Account)) {
            cube.setCount(statistics.getCounts().get(BusinessEntity.Account));
        }
        return cube;
    }

    public static TopNTree toTopNTree(Statistics statistics) {
        TopNTree topNTree = new TopNTree();
        Map<Category, CategoryTopNTree> catTrees = new HashMap<>();
        for (Map.Entry<Category, CategoryStatistics> entry: statistics.getCategories().entrySet()) {
            catTrees.put(entry.getKey(), toCatTopTree(entry.getValue()));
        }
        topNTree.setCategories(catTrees);
        return topNTree;
    }


    private static CategoryTopNTree toCatTopTree(CategoryStatistics catStats) {
        CategoryTopNTree topNTree = new CategoryTopNTree();
        Map<String, List<TopAttribute>> subCatTrees = new HashMap<>();
        for (Map.Entry<String, SubcategoryStatistics> entry: catStats.getSubcategories().entrySet()) {
            subCatTrees.put(entry.getKey(), toSubcatTopTree(entry.getValue()));
        }
        topNTree.setSubcategories(subCatTrees);
        return topNTree;
    }

    private static List<TopAttribute> toSubcatTopTree(SubcategoryStatistics catStats) {
        return catStats.getAttributes().entrySet().stream() //
                .sorted(Comparator.comparing(entry -> - entry.getValue().getNonNullCount())) //
                .map(StatsCubeUtils::toTopAttr) //
                .collect(Collectors.toList());
    }

    private static TopAttribute toTopAttr(Map.Entry<AttributeLookup, AttributeStats> entry) {
        return new TopAttribute(entry.getKey().getAttribute(), entry.getValue().getNonNullCount());
    }

}
