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

public class AttrStatsDetailsDedupMergeUnitTestNG {
    AttrStatsDetailsMergeTool dedupUtil;

    public AttrStatsDetailsDedupMergeUnitTestNG() {
        dedupUtil = AttrStatsDetailsMergeFactory//
                .getUtil(MergeType.DEDUP);
    }

    @Test
    public void testRegularNullBucketDedup() {
        AttributeStatsDetails firstStatsDetails = new AttributeStatsDetails();
        AttributeStatsDetails secondStatsDetails = new AttributeStatsDetails();

        firstStatsDetails.setNonNullCount(0L);
        secondStatsDetails.setNonNullCount(1L);

        AttributeStatsDetails resultStatsDetails = dedupUtil.merge(firstStatsDetails, secondStatsDetails, true);
        Assert.assertEquals(resultStatsDetails.getNonNullCount(), new Long(1));
        Assert.assertNull(resultStatsDetails.getBuckets());
    }

    @Test
    public void testRegularBucketDedup1() {
        AttributeStatsDetails firstStatsDetails = new AttributeStatsDetails();

        Buckets firstBuckets = new Buckets();
        firstBuckets.setType(BucketType.Boolean);
        List<Bucket> firstBucketList = new ArrayList<>();
        Bucket fb1 = new Bucket();
        fb1.setBucketLabel(Boolean.TRUE.toString());
        fb1.setId(0L);
        fb1.setCount(1L);
        firstBucketList.add(fb1);
        firstBuckets.setBucketList(firstBucketList);
        firstStatsDetails.setBuckets(firstBuckets);
        firstStatsDetails.setNonNullCount(1L);

        AttributeStatsDetails secondStatsDetails = new AttributeStatsDetails();

        Buckets secondBuckets = new Buckets();
        secondBuckets.setType(BucketType.Boolean);
        List<Bucket> secondBucketList = new ArrayList<>();
        Bucket sb1 = new Bucket();
        sb1.setBucketLabel(Boolean.TRUE.toString());
        sb1.setId(0L);
        sb1.setCount(1L);
        secondBucketList.add(sb1);
        secondBuckets.setBucketList(secondBucketList);
        secondStatsDetails.setBuckets(secondBuckets);
        secondStatsDetails.setNonNullCount(1L);

        AttributeStatsDetails resultStatsDetails = dedupUtil.merge(firstStatsDetails, secondStatsDetails, true);
        Assert.assertEquals(resultStatsDetails.getNonNullCount(), new Long(1));
        Assert.assertNotNull(resultStatsDetails.getBuckets());
        Assert.assertEquals(resultStatsDetails.getBuckets().getType(), BucketType.Boolean);
        Assert.assertNotNull(resultStatsDetails.getBuckets().getBucketList());
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().size(), 1);
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(0).getBucketLabel(),
                Boolean.TRUE.toString());
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(0).getId(), new Long(0));
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(0).getCount(), new Long(1));
    }

    @Test
    public void testRegularBucketDedup2() {
        AttributeStatsDetails firstStatsDetails = new AttributeStatsDetails();

        Buckets firstBuckets = new Buckets();
        firstBuckets.setType(BucketType.Boolean);
        List<Bucket> firstBucketList = new ArrayList<>();
        Bucket fb1 = new Bucket();
        fb1.setBucketLabel(Boolean.FALSE.toString());
        fb1.setId(1L);
        fb1.setCount(1L);
        firstBucketList.add(fb1);
        firstBuckets.setBucketList(firstBucketList);
        firstStatsDetails.setBuckets(firstBuckets);
        firstStatsDetails.setNonNullCount(1L);

        AttributeStatsDetails secondStatsDetails = new AttributeStatsDetails();

        Buckets secondBuckets = new Buckets();
        secondBuckets.setType(BucketType.Boolean);
        List<Bucket> secondBucketList = new ArrayList<>();
        Bucket sb1 = new Bucket();
        sb1.setBucketLabel(Boolean.TRUE.toString());
        sb1.setId(0L);
        sb1.setCount(1L);
        secondBucketList.add(sb1);
        secondBuckets.setBucketList(secondBucketList);
        secondStatsDetails.setBuckets(secondBuckets);
        secondStatsDetails.setNonNullCount(1L);

        AttributeStatsDetails resultStatsDetails = dedupUtil.merge(firstStatsDetails, secondStatsDetails, true);
        Assert.assertEquals(resultStatsDetails.getNonNullCount(), new Long(1));
        Assert.assertNotNull(resultStatsDetails.getBuckets());
        Assert.assertEquals(resultStatsDetails.getBuckets().getType(), BucketType.Boolean);
        Assert.assertNotNull(resultStatsDetails.getBuckets().getBucketList());
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().size(), 1);
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(0).getBucketLabel(),
                Boolean.TRUE.toString());
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(0).getId(), new Long(0));
        Assert.assertEquals(resultStatsDetails.getBuckets().getBucketList().get(0).getCount(), new Long(1));
    }

    @Test
    public void testEncodedBucketDedup1() {
        AttributeStatsDetails firstStatsDetails = new AttributeStatsDetails();

        Buckets firstBuckets = new Buckets();
        firstBuckets.setType(BucketType.Boolean);
        List<Bucket> firstBucketList = new ArrayList<>();
        Bucket fb1 = new Bucket();
        fb1.setBucketLabel(Boolean.TRUE.toString());
        fb1.setId(0L);
        Long[] firstEncodedTrueCountList = new Long[] { 0L, 0L, 0L, 0L };
        fb1.setEncodedCountList(firstEncodedTrueCountList);
        firstBucketList.add(fb1);
        Bucket fb2 = new Bucket();
        fb2.setBucketLabel(Boolean.FALSE.toString());
        fb2.setId(1L);
        Long[] firstEncodedFalseCountList = new Long[] { 1L, 1L, 1L, 1L };
        fb2.setEncodedCountList(firstEncodedFalseCountList);
        firstBucketList.add(fb2);
        firstBuckets.setBucketList(firstBucketList);
        firstStatsDetails.setBuckets(firstBuckets);
        firstStatsDetails.setNonNullCount(1L);

        AttributeStatsDetails secondStatsDetails = new AttributeStatsDetails();

        Buckets secondBuckets = new Buckets();
        secondBuckets.setType(BucketType.Boolean);
        List<Bucket> secondBucketList = new ArrayList<>();
        Bucket sb1 = new Bucket();
        sb1.setBucketLabel(Boolean.TRUE.toString());
        sb1.setId(0L);
        Long[] secondEncodedTrueCountList = new Long[] { 1L, 1L, 1L, 1L };
        sb1.setEncodedCountList(secondEncodedTrueCountList);
        secondBucketList.add(sb1);
        Bucket sb2 = new Bucket();
        sb2.setBucketLabel(Boolean.FALSE.toString());
        sb2.setId(1L);
        Long[] secondEncodedFalseCountList = new Long[] { 0L, 0L, 0L, 0L };
        sb2.setEncodedCountList(secondEncodedFalseCountList);
        secondBucketList.add(sb2);
        secondBuckets.setBucketList(secondBucketList);
        secondStatsDetails.setBuckets(secondBuckets);
        secondStatsDetails.setNonNullCount(1L);

        AttributeStatsDetails resultStatsDetails = dedupUtil.merge(firstStatsDetails, secondStatsDetails, true);
        Assert.assertEquals(resultStatsDetails.getNonNullCount(), new Long(1));
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
                    new Long(1));
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
                    new Long(0));
        }
    }

    @Test
    public void testEncodedBucketDedup2() {
        AttributeStatsDetails firstStatsDetails = new AttributeStatsDetails();

        Buckets firstBuckets = new Buckets();
        firstBuckets.setType(BucketType.Boolean);
        List<Bucket> firstBucketList = new ArrayList<>();
        Bucket fb1 = new Bucket();
        fb1.setBucketLabel(Boolean.TRUE.toString());
        fb1.setId(0L);
        Long[] firstEncodedTrueCountList = new Long[] { 1L, 0L, 1L, 0L };
        fb1.setEncodedCountList(firstEncodedTrueCountList);
        firstBucketList.add(fb1);
        Bucket fb2 = new Bucket();
        fb2.setBucketLabel(Boolean.FALSE.toString());
        fb2.setId(1L);
        Long[] firstEncodedFalseCountList = new Long[] { 0L, 1L, 0L, 1L };
        fb2.setEncodedCountList(firstEncodedFalseCountList);
        firstBucketList.add(fb2);
        firstBuckets.setBucketList(firstBucketList);
        firstStatsDetails.setBuckets(firstBuckets);
        firstStatsDetails.setNonNullCount(1L);

        AttributeStatsDetails secondStatsDetails = new AttributeStatsDetails();

        Buckets secondBuckets = new Buckets();
        secondBuckets.setType(BucketType.Boolean);
        List<Bucket> secondBucketList = new ArrayList<>();
        Bucket sb1 = new Bucket();
        sb1.setBucketLabel(Boolean.TRUE.toString());
        sb1.setId(0L);
        Long[] secondEncodedTrueCountList = new Long[] { 0L, 0L, 1L, 1L };
        sb1.setEncodedCountList(secondEncodedTrueCountList);
        secondBucketList.add(sb1);
        Bucket sb2 = new Bucket();
        sb2.setBucketLabel(Boolean.FALSE.toString());
        sb2.setId(1L);
        Long[] secondEncodedFalseCountList = new Long[] { 1L, 1L, 0L, 0L };
        sb2.setEncodedCountList(secondEncodedFalseCountList);
        secondBucketList.add(sb2);
        secondBuckets.setBucketList(secondBucketList);
        secondStatsDetails.setBuckets(secondBuckets);
        secondStatsDetails.setNonNullCount(1L);

        Long[] expectedTrueCountList = new Long[] { 1L, 0L, 1L, 1L };
        Long[] expectedFalseCountList = new Long[] { 0L, 1L, 0L, 0L };

        AttributeStatsDetails resultStatsDetails = dedupUtil.merge(firstStatsDetails, secondStatsDetails, true);
        Assert.assertEquals(resultStatsDetails.getNonNullCount(), new Long(1));
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
                    expectedTrueCountList[i]);
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
                    expectedFalseCountList[i]);
        }
    }
}
