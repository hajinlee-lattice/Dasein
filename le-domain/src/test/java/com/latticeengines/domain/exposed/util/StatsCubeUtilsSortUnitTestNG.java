package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.RatingEngine;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class StatsCubeUtilsSortUnitTestNG {

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

    private void verifyAttrSeq(Flux<ColumnMetadata> flux, Iterable<String> attrSeq1, Iterable<String> attrSeq2) {
        flux = flux.sort(Comparator.comparing(ColumnMetadata::getCategory)).groupBy(ColumnMetadata::getCategory).flatMapSequential(grp -> grp);
        StepVerifier.FirstStep<ColumnMetadata> verifier = StepVerifier.create(flux);
        for (String nextAttr : attrSeq1) {
            verifier.consumeNextWith(cm -> {
                System.out.println(cm.getAttrName() + " : " + cm.getImportanceOrdering());
                Assert.assertEquals(cm.getAttrName(), nextAttr,
                        "Next attributes should be " + nextAttr + " but actually found " + cm.getAttrName());
            });
        }
        for (String nextAttr : attrSeq2) {
            verifier.consumeNextWith(cm -> {
                System.out.println(idSeqMap.get(RatingEngine.toEngineId(cm.getAttrName())) + " : " + cm.getImportanceOrdering());
                Assert.assertEquals(cm.getAttrName(), nextAttr,
                        "Next attributes should be " + nextAttr + " but actually found " + cm.getAttrName());
            });
        }
        verifier.verifyComplete();
    }

}
