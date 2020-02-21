package com.latticeengines.domain.exposed.util;

import static com.latticeengines.domain.exposed.query.BusinessEntity.WebVisitProfile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.CategoryTopNTree;
import com.latticeengines.domain.exposed.metadata.statistics.TopAttribute;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class StatsCubeUtilsSortUnitTestNG {

    private static final String TEST_SUB_CATEGORY = "test-subcategory";
    Map<String, Integer> idSeqMap = new HashMap<>();

    @Test(groups = "unit")
    public void testSortFirmographics() {
        Map<String, AttributeStats> cube = new HashMap<>();
        List<ColumnMetadata> cmList = new ArrayList<>();

        String attrName = "LDC_PrimaryIndustry";
        ColumnMetadata metadata = new ColumnMetadata();
        metadata.setAttrName(attrName);
        cmList.add(metadata);
        AttributeStats attrStats = new AttributeStats();
        attrStats.setNonNullCount(30L);
        Buckets buckets = new Buckets();
        buckets.setType(BucketType.Enum);
        List<Bucket> bucketList = new ArrayList<>();
        Bucket bucket1 = Bucket.valueBkt("Ind1");
        bucket1.setCount(10L);
        bucketList.add(bucket1);
        Bucket bucket2 = Bucket.valueBkt("Ind1");
        bucket2.setCount(20L);
        bucketList.add(bucket2);
        buckets.setBucketList(bucketList);
        attrStats.setBuckets(buckets);
        cube.put(attrName, attrStats);

        attrName = "Attr2";
        metadata = new ColumnMetadata();
        metadata.setAttrName(attrName);
        cmList.add(metadata);
        attrStats = new AttributeStats();
        attrStats.setNonNullCount(50L);
        buckets = new Buckets();
        buckets.setType(BucketType.Numerical);
        bucketList = new ArrayList<>();
        bucket1 = Bucket.rangeBkt(1, 10);
        bucket1.setCount(30L);
        bucketList.add(bucket1);
        bucket2 = Bucket.rangeBkt(10, 100);
        bucket2.setCount(20L);
        bucketList.add(bucket2);
        buckets.setBucketList(bucketList);
        attrStats.setBuckets(buckets);
        cube.put(attrName, attrStats);

        attrName = "Attr1";
        metadata = new ColumnMetadata();
        metadata.setAttrName(attrName);
        cmList.add(metadata);
        attrStats = new AttributeStats();
        attrStats.setNonNullCount(30L);
        buckets = new Buckets();
        buckets.setType(BucketType.Enum);
        bucketList = new ArrayList<>();
        bucket1 = Bucket.valueBkt("Val1");
        bucket1.setCount(10L);
        bucketList.add(bucket1);
        bucket2 = Bucket.valueBkt("Val2");
        bucket2.setCount(20L);
        bucketList.add(bucket2);
        buckets.setBucketList(bucketList);
        attrStats.setBuckets(buckets);
        cube.put(attrName, attrStats);

        attrName = "LE_NUMBER_OF_LOCATIONS";
        metadata = new ColumnMetadata();
        metadata.setAttrName(attrName);
        cmList.add(metadata);
        attrStats = new AttributeStats();
        attrStats.setNonNullCount(50L);
        buckets = new Buckets();
        buckets.setType(BucketType.Numerical);
        bucketList = new ArrayList<>();
        bucket1 = Bucket.rangeBkt(1, 10);
        bucket1.setCount(30L);
        bucketList.add(bucket1);
        bucket2 = Bucket.rangeBkt(10, 100);
        bucket2.setCount(20L);
        bucketList.add(bucket2);
        buckets.setBucketList(bucketList);
        attrStats.setBuckets(buckets);
        cube.put(attrName, attrStats);

        attrName = "Attr3";
        metadata = new ColumnMetadata();
        metadata.setAttrName(attrName);
        cmList.add(metadata);
        attrStats = new AttributeStats();
        attrStats.setNonNullCount(30L);
        buckets = new Buckets();
        buckets.setType(BucketType.Enum);
        attrStats.setBuckets(buckets);
        cube.put(attrName, attrStats);

        attrName = "Attr4";
        metadata = new ColumnMetadata();
        metadata.setAttrName(attrName);
        cmList.add(metadata);
        attrStats = new AttributeStats();
        attrStats.setNonNullCount(90L);
        buckets = new Buckets();
        buckets.setType(BucketType.Numerical);
        attrStats.setBuckets(buckets);
        cube.put(attrName, attrStats);

        List<ColumnMetadata> cmList2 = new ArrayList<>();
        String engineId1 = RatingEngine.generateIdStr();
        String engineId2 = RatingEngine.generateIdStr();
        String engineId3 = RatingEngine.generateIdStr();
        String engineId4 = RatingEngine.generateIdStr();
        String engineId5 = RatingEngine.generateIdStr();

        attrName = engineId1 + "_score";
        metadata = new ColumnMetadata();
        metadata.setAttrName(attrName);
        cmList2.add(metadata);
        attrStats = new AttributeStats();
        attrStats.setNonNullCount(50L);
        buckets = new Buckets();
        buckets.setType(BucketType.Numerical);
        bucketList = new ArrayList<>();
        bucket1 = Bucket.rangeBkt(1, 10);
        bucket1.setCount(30L);
        bucket1.setId(1L);
        bucketList.add(bucket1);
        bucket2 = Bucket.rangeBkt(10, 100);
        bucket2.setCount(20L);
        bucket2.setId(2L);
        bucketList.add(bucket2);
        buckets.setBucketList(bucketList);
        attrStats.setBuckets(buckets);
        cube.put(attrName, attrStats);

        attrName = engineId2;
        metadata = new ColumnMetadata();
        metadata.setAttrName(attrName);
        cmList2.add(metadata);
        attrStats = new AttributeStats();
        attrStats.setNonNullCount(30L);
        buckets = new Buckets();
        buckets.setType(BucketType.Enum);
        bucketList = new ArrayList<>();
        bucket1 = Bucket.valueBkt("A");
        bucket1.setCount(10L);
        bucket1.setId(1L);
        bucketList.add(bucket1);
        bucket2 = Bucket.valueBkt("B");
        bucket2.setCount(20L);
        bucket2.setId(2L);
        bucketList.add(bucket2);
        buckets.setBucketList(bucketList);
        attrStats.setBuckets(buckets);
        cube.put(attrName, attrStats);

        attrName = engineId3;
        metadata = new ColumnMetadata();
        metadata.setAttrName(attrName);
        cmList2.add(metadata);
        attrStats = new AttributeStats();
        attrStats.setNonNullCount(50L);
        buckets = new Buckets();
        buckets.setType(BucketType.Enum);
        bucketList = new ArrayList<>();
        bucket2 = Bucket.valueBkt("B");
        bucket2.setCount(30L);
        bucket2.setId(2L);
        bucketList.add(bucket2);
        buckets.setBucketList(bucketList);
        attrStats.setBuckets(buckets);
        cube.put(attrName, attrStats);

        attrName = engineId4;
        metadata = new ColumnMetadata();
        metadata.setAttrName(attrName);
        cmList2.add(metadata);
        attrStats = new AttributeStats();
        attrStats.setNonNullCount(50L);
        buckets = new Buckets();
        buckets.setType(BucketType.Enum);
        bucketList = new ArrayList<>();
        bucket1 = Bucket.valueBkt("A");
        bucket1.setId(1L);
        bucket1.setCount(30L);
        bucketList.add(bucket1);
        bucket2 = Bucket.valueBkt("B");
        bucket2.setCount(20L);
        bucket2.setId(2L);
        bucketList.add(bucket2);
        buckets.setBucketList(bucketList);
        attrStats.setBuckets(buckets);
        cube.put(attrName, attrStats);

        attrName = engineId5;
        metadata = new ColumnMetadata();
        metadata.setAttrName(attrName);
        cmList2.add(metadata);
        attrStats = new AttributeStats();
        attrStats.setNonNullCount(20L);
        buckets = new Buckets();
        buckets.setType(BucketType.Enum);
        bucketList = new ArrayList<>();
        bucket1 = Bucket.valueBkt("A");
        bucket1.setId(1L);
        bucket1.setCount(20L);
        bucketList.add(bucket1);
        bucketList.add(bucket2);
        buckets.setBucketList(bucketList);
        attrStats.setBuckets(buckets);
        cube.put(attrName, attrStats);


        int seq = 1;
        for (String engineId: Arrays.asList(engineId1, engineId2, engineId3, engineId4, engineId5)) {
            idSeqMap.put(engineId, seq++);
        }

        StatsCube statsCube = new StatsCube();
        statsCube.setStatistics(cube);
        Flux<ColumnMetadata> flux = Flux.fromIterable(cmList).map(cm -> {
            cm.setCategory(Category.FIRMOGRAPHICS);
            return cm;
        });
        flux = flux.concatWith(Flux.fromIterable(cmList2).map(cm -> {
            cm.setCategory(Category.RATING);
            return cm;
        }));
        verifyAttrSeq(StatsCubeUtils.sortByCategory(flux, statsCube),
                Arrays.asList("LDC_PrimaryIndustry", "LE_NUMBER_OF_LOCATIONS", "Attr1", "Attr2", "Attr3", "Attr4"),
                Arrays.asList(engineId4, engineId5, engineId2, engineId3, engineId1 + "_score")
        );
    }

    @Test(groups = "unit", dataProvider = "sortActivityMetrics")
    private void testSortActivityMetrics(List<String> attrs, List<long[][]> valCntList, List<String> expectedOrder,
            long[][] expectedTopBuckets) {
        Assert.assertEquals(attrs.size(), valCntList.size());

        // prepare input
        StatsCube statsCube = new StatsCube();
        Map<String, AttributeStats> stats = new HashMap<>();
        statsCube.setStatistics(stats);
        List<ColumnMetadata> cms = new ArrayList<>();
        int N = attrs.size();
        for (int i = 0; i < N; i++) {
            ColumnMetadata cm = new ColumnMetadata();
            cm.setCategory(Category.WEB_VISIT_PROFILE);
            cm.setAttrName(attrs.get(i));
            cm.setSubcategory(TEST_SUB_CATEGORY);
            cms.add(cm);
            stats.put(attrs.get(i), testStats(valCntList.get(i)));
        }

        // build top n tree
        TopNTree topNTree = StatsCubeUtils.constructTopNTree(ImmutableMap.of(WebVisitProfile.name(), statsCube),
                ImmutableMap.of(WebVisitProfile.name(), cms), true, ColumnSelection.Predefined.Segment, true);
        Assert.assertNotNull(topNTree);
        Assert.assertTrue(topNTree.hasCategory(Category.WEB_VISIT_PROFILE));

        // verify attribute orders
        CategoryTopNTree categoryTopNTree = topNTree.getCategory(Category.WEB_VISIT_PROFILE);
        Assert.assertNotNull(categoryTopNTree);
        List<TopAttribute> topAttrs = categoryTopNTree.getSubcategory(TEST_SUB_CATEGORY);
        Assert.assertNotNull(topAttrs);
        List<String> sortedAttrs = topAttrs.stream().map(TopAttribute::getAttribute).collect(Collectors.toList());
        Assert.assertEquals(sortedAttrs.size(), attrs.size(),
                "sorted attribute list should have the same size as original list");
        Assert.assertEquals(sortedAttrs, expectedOrder);
        // verify bucket value
        for (int i = 0; i < N; i++) {
            TopAttribute attr = topAttrs.get(i);
            long[] expectedBucket = expectedTopBuckets[i];
            String attrName = attr.getAttribute();
            Assert.assertNotNull(attr, String.format("TopAttribute for attr %s should not be null", attrName));
            Assert.assertNotNull(attr.getTopBkt());
            Assert.assertNotNull(attr.getTopBkt().getValues());
            Assert.assertFalse(attr.getTopBkt().getValues().isEmpty());
            long cnt = attr.getTopBkt().getCount();
            long val = Long.valueOf(attr.getTopBkt().getValues().get(0).toString());
            Assert.assertEquals(val, expectedBucket[0],
                    String.format("Value in top bucket for attr %s does not match the expected value", attrName));
            Assert.assertEquals(cnt, expectedBucket[1],
                    String.format("Cnt in top bucket for attr %s does not match the expected value", attrName));
        }
    }

    @DataProvider(name = "sortActivityMetrics")
    private Object[][] sortActivityMetricsTestData() {
        return new Object[][] { //
                { // only one bucket for each attr
                        Arrays.asList("a1", "a2", "a3", "a4"), //
                        Arrays.asList( //
                                new long[][] { { 10, 5 } }, //
                                new long[][] { { 90, 5 } }, //
                                new long[][] { { 500, 5 } }, //
                                new long[][] { { 20, 500 } } //
                        ), //
                        Arrays.asList("a3", "a2", "a4", "a1"), //
                        new long[][] { { 500, 5 }, { 90, 5 }, { 20, 500 }, { 10, 5 } }, //
                }, //
                { // multiple bucket, sorted in attr first
                        Arrays.asList("a1", "a2", "a3", "a4"), //
                        Arrays.asList( //
                                new long[][] { { 100, 3 }, { 90, 5 } }, //
                                new long[][] { { 90, 5 }, { 90, 10 }, { 30, 100 } }, //
                                new long[][] { { 90, 5 }, { 30, 100 } }, //
                                new long[][] { { 30, 100 }, { 20, 500 } } //
                        ), //
                        Arrays.asList("a1", "a2", "a3", "a4"), //
                        new long[][] { { 100, 3 }, { 90, 10 }, { 90, 5 }, { 30, 100 } }, //
                }, //
                { //
                        Arrays.asList("a1", "a2", "a3", "a4"), //
                        Arrays.asList( //
                                new long[][] { { 150, 5 } }, //
                                new long[][] { { 90, 5 }, { 90, 10 }, { 30, 100 } }, //
                                new long[][] { { 30, 5 }, { 30, 100 }, { 10, 150 } }, //
                                new long[][] { { 30, 90 }, { 20, 500 }, { 30, 120 } } //
                        ), //
                        Arrays.asList("a1", "a2", "a4", "a3"), //
                        new long[][] { { 150, 5 }, { 90, 10 }, { 30, 120 }, { 30, 100 } }, //
                }, //
                { //
                        Arrays.asList("a1", "a2", "a3", "a4", "a5"), //
                        Arrays.asList( //
                                new long[][] { { 30, 5 }, { 70, 100 }, { 70, 150 } }, //
                                new long[][] { { 150, 5 } }, //
                                new long[][] { { 30, 5 }, { 30, 10 } }, //
                                new long[][] { { 30, 5 }, { 30, 100 }, { 10, 150 } }, //
                                new long[][] { { 200, 90 } } //
                        ), //
                        Arrays.asList("a5", "a2", "a1", "a4", "a3"), //
                        new long[][] { { 200, 90 }, { 150, 5 }, { 70, 150 }, { 30, 100 }, { 30, 10 } }, //
                }, //
        }; //
    }

    private AttributeStats testStats(long[][] valCnts) {
        AttributeStats stats = new AttributeStats();
        stats.setNonNullCount((long) valCnts.length);
        Buckets bkts = new Buckets();
        stats.setBuckets(bkts);
        bkts.setType(BucketType.Numerical);
        List<Bucket> bucketList = new ArrayList<>();
        for (long[] vc : valCnts) {
            bucketList.add(testBucket(vc[0], vc[1]));
        }
        bkts.setBucketList(bucketList);
        return stats;
    }

    private Bucket testBucket(long value, long count) {
        Bucket bkt = Bucket.valueBkt(String.valueOf(value));
        bkt.setCount(count);
        bkt.setId(0L);
        return bkt;
    }

    private void verifyAttrSeq(Flux<ColumnMetadata> flux, Iterable<String> attrSeq1, Iterable<String> attrSeq2) {
        flux = flux.sort(Comparator.comparing(ColumnMetadata::getCategory)).groupBy(ColumnMetadata::getCategory).flatMapSequential(grp -> grp);
        StepVerifier.FirstStep<ColumnMetadata> verifier = StepVerifier.create(flux);
        for (String nextAttr : attrSeq2) {
            verifier.consumeNextWith(cm -> {
                System.out.println(idSeqMap.get(RatingEngine.toEngineId(cm.getAttrName())) + " : " + cm.getImportanceOrdering());
                Assert.assertEquals(cm.getAttrName(), nextAttr,
                        "Next attributes should be " + nextAttr + " but actually found " + cm.getAttrName());
            });
        }
        for (String nextAttr : attrSeq1) {
            verifier.consumeNextWith(cm -> {
                System.out.println(cm.getAttrName() + " : " + cm.getImportanceOrdering());
                Assert.assertEquals(cm.getAttrName(), nextAttr,
                        "Next attributes should be " + nextAttr + " but actually found " + cm.getAttrName());
            });
        }
        verifier.verifyComplete();
    }

}
