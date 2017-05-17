package com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket.AttrStatsDetailsMergeFactory.MergeType;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStatsDetails;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;

public class AttrStatsDetailsAddMergeUnitTestNG {
    AttrStatsDetailsMergeTool addUtil;

    public AttrStatsDetailsAddMergeUnitTestNG() {
        addUtil = AttrStatsDetailsMergeFactory//
                .getUtil(MergeType.ADD);
    }

    @Test
    public void testRegularNullBucketAdd() {
        AttributeStatsDetails firstStatsDetails = new AttributeStatsDetails();
        AttributeStatsDetails secondStatsDetails = new AttributeStatsDetails();

        firstStatsDetails.setNonNullCount(20L);
        secondStatsDetails.setNonNullCount(30L);

        AttributeStatsDetails resultStatsDetails = addUtil.merge(firstStatsDetails, secondStatsDetails, true);
        Assert.assertEquals(resultStatsDetails.getNonNullCount(), new Long(50));
        Assert.assertNull(resultStatsDetails.getBuckets());
    }

    @Test
    public void testRegularBucketAdd1() {
        AttributeStatsDetails firstStatsDetails = new AttributeStatsDetails();

        Buckets firstBuckets = new Buckets();
        firstBuckets.setType(BucketType.Boolean);
        List<Bucket> firstBucketList = new ArrayList<>();
        Bucket fb1 = new Bucket();
        fb1.setBucketLabel(Boolean.FALSE.toString());
        fb1.setId(1L);
        fb1.setCount(20L);
        firstBucketList.add(fb1);
        firstBuckets.setBucketList(firstBucketList);
        firstStatsDetails.setBuckets(firstBuckets);
        firstStatsDetails.setNonNullCount(20L);

        AttributeStatsDetails secondStatsDetails = new AttributeStatsDetails();

        Buckets secondBuckets = new Buckets();
        secondBuckets.setType(BucketType.Boolean);
        List<Bucket> secondBucketList = new ArrayList<>();
        Bucket sb1 = new Bucket();
        sb1.setBucketLabel(Boolean.TRUE.toString());
        sb1.setId(0L);
        sb1.setCount(30L);
        secondBucketList.add(sb1);
        secondBuckets.setBucketList(secondBucketList);
        secondStatsDetails.setBuckets(secondBuckets);
        secondStatsDetails.setNonNullCount(30L);

        AttributeStatsDetails resultStatsDetails = addUtil.merge(firstStatsDetails, secondStatsDetails, true);
        Assert.assertEquals(resultStatsDetails.getNonNullCount(), new Long(50));
        Assert.assertNotNull(resultStatsDetails.getBuckets());
        Assert.assertEquals(resultStatsDetails.getBuckets().getType(), BucketType.Boolean);
        Assert.assertNotNull(resultStatsDetails.getBuckets().getBucketList());
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().size(), 2);
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(0).getBucketLabel(),
                Boolean.TRUE.toString());
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(0).getId(), new Long(0));
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(0).getCount(), new Long(30));
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(1).getBucketLabel(),
                Boolean.FALSE.toString());
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(1).getId(), new Long(1));
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(1).getCount(), new Long(20));
    }

    @Test
    public void testRegularBucketAdd2() {
        AttributeStatsDetails firstStatsDetails = new AttributeStatsDetails();

        Buckets firstBuckets = new Buckets();
        firstBuckets.setType(BucketType.Numerical);
        List<Bucket> firstBucketList = new ArrayList<>();
        Bucket fb1 = new Bucket();
        fb1.setBucketLabel("100-200");
        fb1.setId(1L);
        fb1.setCount(10L);
        firstBucketList.add(fb1);
        Bucket fb2 = new Bucket();
        fb2.setBucketLabel("200-300");
        fb2.setId(2L);
        fb2.setCount(10L);
        firstBucketList.add(fb2);
        firstBuckets.setBucketList(firstBucketList);
        firstStatsDetails.setBuckets(firstBuckets);
        firstStatsDetails.setNonNullCount(20L);

        AttributeStatsDetails secondStatsDetails = new AttributeStatsDetails();

        Buckets secondBuckets = new Buckets();
        secondBuckets.setType(BucketType.Numerical);
        List<Bucket> secondBucketList = new ArrayList<>();
        Bucket sb1 = new Bucket();
        sb1.setBucketLabel("0-100");
        sb1.setId(0L);
        sb1.setCount(20L);
        secondBucketList.add(sb1);
        Bucket sb2 = new Bucket();
        sb2.setBucketLabel("100-200");
        sb2.setId(1L);
        sb2.setCount(5L);
        secondBucketList.add(sb2);
        Bucket fb3 = new Bucket();
        fb3.setBucketLabel("200-300");
        fb3.setId(2L);
        fb3.setCount(5L);
        secondBucketList.add(fb3);
        secondBuckets.setBucketList(secondBucketList);
        secondStatsDetails.setBuckets(secondBuckets);
        secondStatsDetails.setNonNullCount(30L);

        AttributeStatsDetails resultStatsDetails = addUtil.merge(firstStatsDetails, secondStatsDetails, true);
        Assert.assertEquals(resultStatsDetails.getNonNullCount(), new Long(50));
        Assert.assertNotNull(resultStatsDetails.getBuckets());
        Assert.assertEquals(resultStatsDetails.getBuckets().getType(), BucketType.Numerical);
        Assert.assertNotNull(resultStatsDetails.getBuckets().getBucketList());
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().size(), 3);
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(0).getBucketLabel(), "0-100");
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(0).getId(), new Long(0));
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(0).getCount(), new Long(20));
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(1).getBucketLabel(), "100-200");
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(1).getId(), new Long(1));
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(1).getCount(), new Long(15));
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(2).getBucketLabel(), "200-300");
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(2).getId(), new Long(2));
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(2).getCount(), new Long(15));
    }

    @Test
    public void testEncodedBucketAdd1() {
        AttributeStatsDetails firstStatsDetails = new AttributeStatsDetails();

        Buckets firstBuckets = new Buckets();
        firstBuckets.setType(BucketType.Boolean);
        List<Bucket> firstBucketList = new ArrayList<>();
        Bucket fb1 = new Bucket();
        fb1.setBucketLabel(Boolean.FALSE.toString());
        fb1.setId(1L);
        Long[] firstEncodedFalseCountList = new Long[] { 20L, 20L, 20L, 20L };
        fb1.setEncodedCountList(firstEncodedFalseCountList);
        firstBucketList.add(fb1);
        firstBuckets.setBucketList(firstBucketList);
        firstStatsDetails.setBuckets(firstBuckets);
        firstStatsDetails.setNonNullCount(20L);

        AttributeStatsDetails secondStatsDetails = new AttributeStatsDetails();

        Buckets secondBuckets = new Buckets();
        secondBuckets.setType(BucketType.Boolean);
        List<Bucket> secondBucketList = new ArrayList<>();
        Bucket sb1 = new Bucket();
        sb1.setBucketLabel(Boolean.TRUE.toString());
        sb1.setId(0L);
        Long[] secondEncodedTrueCountList = new Long[] { 30L, 30L, 30L, 30L };
        sb1.setEncodedCountList(secondEncodedTrueCountList);
        secondBucketList.add(sb1);
        secondBuckets.setBucketList(secondBucketList);
        secondStatsDetails.setBuckets(secondBuckets);
        secondStatsDetails.setNonNullCount(30L);

        AttributeStatsDetails resultStatsDetails = addUtil.merge(firstStatsDetails, secondStatsDetails, true);
        Assert.assertEquals(resultStatsDetails.getNonNullCount(), new Long(50));
        Assert.assertNotNull(resultStatsDetails.getBuckets());
        Assert.assertEquals(resultStatsDetails.getBuckets().getType(), BucketType.Boolean);
        Assert.assertNotNull(resultStatsDetails.getBuckets().getBucketList());
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().size(), 2);
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(0).getBucketLabel(),
                Boolean.TRUE.toString());
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(0).getId(), new Long(0));
        Assert.assertNull(resultStatsDetails.getBuckets().getBucketList().get(0).getCount());
        Assert.assertNotNull(resultStatsDetails.getBuckets().getBucketList().get(0).getEncodedCountList());
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(0).getEncodedCountList().length,
                firstEncodedFalseCountList.length);
        for (int i = 0; i < firstEncodedFalseCountList.length; i++) {
            Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(0).getEncodedCountList()[i],
                    new Long(30));
        }
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(1).getBucketLabel(),
                Boolean.FALSE.toString());
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(1).getId(), new Long(1));
        Assert.assertNull(resultStatsDetails.getBuckets().getBucketList().get(1).getCount());
        Assert.assertNotNull(resultStatsDetails.getBuckets().getBucketList().get(1).getEncodedCountList());
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(1).getEncodedCountList().length,
                firstEncodedFalseCountList.length);
        for (int i = 0; i < firstEncodedFalseCountList.length; i++) {
            Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(1).getEncodedCountList()[i],
                    new Long(20));
        }

    }

    @Test
    public void testEncodedBucketAdd2() {
        AttributeStatsDetails firstStatsDetails = new AttributeStatsDetails();

        Buckets firstBuckets = new Buckets();
        firstBuckets.setType(BucketType.Boolean);
        List<Bucket> firstBucketList = new ArrayList<>();
        Bucket fb1 = new Bucket();
        fb1.setBucketLabel(Boolean.TRUE.toString());
        fb1.setId(0L);
        Long[] firstEncodedTrueCountList = new Long[] { 20L, 10L, 0L, 5L };
        fb1.setEncodedCountList(firstEncodedTrueCountList);
        firstBucketList.add(fb1);
        Bucket fb2 = new Bucket();
        fb2.setBucketLabel(Boolean.FALSE.toString());
        fb2.setId(1L);
        Long[] firstEncodedFalseCountList = new Long[] { 0L, 10L, 20L, 15L };
        fb2.setEncodedCountList(firstEncodedFalseCountList);
        firstBucketList.add(fb2);
        firstBuckets.setBucketList(firstBucketList);
        firstStatsDetails.setBuckets(firstBuckets);
        firstStatsDetails.setNonNullCount(20L);

        AttributeStatsDetails secondStatsDetails = new AttributeStatsDetails();

        Buckets secondBuckets = new Buckets();
        secondBuckets.setType(BucketType.Boolean);
        List<Bucket> secondBucketList = new ArrayList<>();
        Bucket sb1 = new Bucket();
        sb1.setBucketLabel(Boolean.TRUE.toString());
        sb1.setId(0L);
        Long[] secondEncodedTrueCountList = new Long[] { 3L, 0L, 15L, 30L };
        sb1.setEncodedCountList(secondEncodedTrueCountList);
        secondBucketList.add(sb1);
        Bucket sb2 = new Bucket();
        sb2.setBucketLabel(Boolean.FALSE.toString());
        sb2.setId(1L);
        Long[] secondEncodedFalseCountList = new Long[] { 27L, 30L, 15L, 0L };
        sb2.setEncodedCountList(secondEncodedFalseCountList);
        secondBucketList.add(sb2);
        secondBuckets.setBucketList(secondBucketList);
        secondStatsDetails.setBuckets(secondBuckets);
        secondStatsDetails.setNonNullCount(30L);

        Long[] expectedTrueCountList = new Long[] { 23L, 10L, 15L, 35L };
        Long[] expectedFalseCountList = new Long[] { 27L, 40L, 35L, 15L };

        AttributeStatsDetails resultStatsDetails = addUtil.merge(firstStatsDetails, secondStatsDetails, true);
        Assert.assertEquals(resultStatsDetails.getNonNullCount(), new Long(50));
        Assert.assertNotNull(resultStatsDetails.getBuckets());
        Assert.assertEquals(resultStatsDetails.getBuckets().getType(), BucketType.Boolean);
        Assert.assertNotNull(resultStatsDetails.getBuckets().getBucketList());
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().size(), 2);
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(0).getBucketLabel(),
                Boolean.TRUE.toString());
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(0).getId(), new Long(0));
        // Assert.assertNull(resultStatsDetails.getBuckets().getBucketList().get(0).getCount());
        Assert.assertNotNull(resultStatsDetails.getBuckets().getBucketList().get(0).getEncodedCountList());
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(0).getEncodedCountList().length,
                firstEncodedFalseCountList.length);
        for (int i = 0; i < firstEncodedFalseCountList.length; i++) {
            Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(0).getEncodedCountList()[i],
                    expectedTrueCountList[i]);
        }
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(1).getBucketLabel(),
                Boolean.FALSE.toString());
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(1).getId(), new Long(1));
        // Assert.assertNull(resultStatsDetails.getBuckets().getBucketList().get(1).getCount());
        Assert.assertNotNull(resultStatsDetails.getBuckets().getBucketList().get(1).getEncodedCountList());
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(1).getEncodedCountList().length,
                firstEncodedFalseCountList.length);
        for (int i = 0; i < firstEncodedFalseCountList.length; i++) {
            Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(1).getEncodedCountList()[i],
                    expectedFalseCountList[i]);
        }

    }
}
