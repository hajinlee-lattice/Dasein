package com.latticeengines.domain.exposed.util;

import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;
import static com.latticeengines.domain.exposed.query.ComparisonType.IN_COLLECTION;
import static com.latticeengines.domain.exposed.query.ComparisonType.IS_NOT_NULL;
import static com.latticeengines.domain.exposed.query.ComparisonType.NOT_IN_COLLECTION;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.MetricRestriction;
import com.latticeengines.domain.exposed.query.Restriction;

public class RestrictionOptimizerUnitTestNG {

    private static final Restriction A1 = bucket(Account, 1);
    private static final Restriction A2 = bucket(Account, 2);
    private static final Restriction A3 = bucket(Account, 3);
    private static final Restriction A4 = bucket(Account, 4);
    private static final Restriction A5 = bucket(Account, 5);
    private static final Restriction C1 = bucket(Contact, 1);
    private static final Restriction C2 = bucket(Contact, 2);
    private static final Restriction C3 = bucket(Contact, 3);
    private static final Restriction C4 = bucket(Contact, 4);
    private static final Restriction C5 = bucket(Contact, 5); // ignored

    @Test(groups = "unit", dataProvider = "optimizeTestData")
    public void testOptimize(Restriction restriction, Restriction expected) {
        Restriction flatten = RestrictionOptimizer.optimize(restriction);
        if (expected == null) {
            Assert.assertNull(flatten);
        } else {
            Assert.assertEquals(JsonUtils.serialize(flatten), JsonUtils.serialize(expected));
        }
    }

    @DataProvider(name = "optimizeTestData")
    public Object[][] provideOptimizeTestData() {
        Restriction r1 = and(A1, and(A2), and(A3, A4));
        Restriction e1 = and(A1, A2, A3, A4);

        Restriction r2 = or(and(A1, A2), and(A3, A4));
        Restriction e2 = or(and(A1, A2), and(A3, A4));

        Restriction r3 = or(or(C1, C2), and(A3, A4), A5);
        Restriction e3 = or(C1, C2, and(A3, A4), A5);

        Restriction r4 = and(C1, or(A2), and(C3, or(C4, A5)));
        Restriction e4 = and(C1, A2, C3, or(C4, A5));

        Restriction r5 = and(C5);
        Restriction e5 = null;

        Restriction r6 = and(C4, C5);
        Restriction e6 = C4;

        Restriction r7 = or(C4, C5);
        Restriction e7 = C4;

        Restriction r8 = or(C1, and(C5, or(C4, A5)));
        Restriction e8 = or(C1, C4, A5);

        Restriction r9 = and(C5, or(C4, A5));
        Restriction e9 = or(C4, A5);

        return new Object[][] { //
                { r1, e1 }, //
                { r2, e2 }, //
                { r3, e3 }, //
                { r4, e4 }, //
                { r5, e5 }, //
                { r6, e6 }, //
                { r7, e7 }, //
                { r8, e8 }, //
                { r9, e9 }, //
        };
    }

    @Test(groups = "unit")
    public void testMergeList() {
        final Restriction AList1 = listBucket(true, 1, 2, 3);
        final Restriction AList2 = listBucket(true, 2, 3, 4);
        final Restriction AList3 = listBucket(false, 10, 20, 30);
        final Restriction AList4 = listBucket(false, 30, 40, 50);
        Restriction res1 = and(AList1, AList2, AList3, AList4);
        Restriction flatten = RestrictionOptimizer.optimize(res1);
        Assert.assertEquals(((LogicalRestriction) flatten).getRestrictions().size(), 2);
        for (Restriction r: ((LogicalRestriction) flatten).getRestrictions()) {
            Bucket bkt = ((BucketRestriction) r).getBkt();
            if (IN_COLLECTION.equals(bkt.getComparisonType())) {
                Assert.assertEquals(bkt.getValues().size(), 2); // 2, 3
            } else {
                Assert.assertEquals(bkt.getValues().size(), 5); // 10, 20, 30, 40, 50
            }
        }

        res1 = or(AList1, AList2, AList3, AList4);
        flatten = RestrictionOptimizer.optimize(res1);
        Assert.assertEquals(((LogicalRestriction) flatten).getRestrictions().size(), 2);
        for (Restriction r: ((LogicalRestriction) flatten).getRestrictions()) {
            Bucket bkt = ((BucketRestriction) r).getBkt();
            if (IN_COLLECTION.equals(bkt.getComparisonType())) {
                Assert.assertEquals(bkt.getValues().size(), 4); // 1, 2, 3, 4
            } else {
                Assert.assertEquals(bkt.getValues().size(), 1); // 30
            }
        }

        final Restriction AList5 = listBucket(true, 1, 2);
        final Restriction AList6 = listBucket(true,  3, 4);
        final Restriction AList7 = listBucket(false, 10, 20, 30);
        final Restriction AList8 = listBucket(false, 40, 50);
        res1 = and(AList5, AList6, AList7, AList8);
        flatten = RestrictionOptimizer.optimize(res1);
        Assert.assertEquals(((LogicalRestriction) flatten).getRestrictions().size(), 2); // one of them is not null
        for (Restriction r: ((LogicalRestriction) flatten).getRestrictions()) {
            Bucket bkt = ((BucketRestriction) r).getBkt();
            Assert.assertTrue(IS_NOT_NULL.equals(bkt.getComparisonType()) || NOT_IN_COLLECTION.equals(bkt.getComparisonType()));
        }

        res1 = or(AList5, AList6, AList7, AList8);
        flatten = RestrictionOptimizer.optimize(res1);
        Assert.assertEquals(((LogicalRestriction) flatten).getRestrictions().size(), 2); // one of them is not null
        for (Restriction r: ((LogicalRestriction) flatten).getRestrictions()) {
            Bucket bkt = ((BucketRestriction) r).getBkt();
            Assert.assertTrue(IS_NOT_NULL.equals(bkt.getComparisonType()) || IN_COLLECTION.equals(bkt.getComparisonType()));
        }
    }

    @Test(groups = "unit", dataProvider = "nullTestData")
    public void testNull(Restriction restriction) {
        Assert.assertNull(RestrictionOptimizer.optimize(restriction));
    }

    @DataProvider(name = "nullTestData")
    public Object[][] provideNullTestData() {
        return new Object[][] { //
                { null }, //
                { and() }, //
                { or() }, //
                { and(and(), or()) }, //
        };
    }

    @Test(groups = "unit", dataProvider = "groupMetricsTestData")
    public void testGroupMetrics(Restriction restriction, Restriction expected) {
        Restriction grouped = RestrictionOptimizer.groupMetrics(RestrictionOptimizer.optimize(restriction));
        String actualStr = JsonUtils.serialize(grouped);
        String expectedStr = JsonUtils.serialize(RestrictionOptimizer.optimize(expected));
        Assert.assertEquals(actualStr, expectedStr);
    }

    @DataProvider(name = "groupMetricsTestData")
    public Object[][] provideGroupMetricsTestData() {
        Restriction m1 = Restriction.builder().let(BusinessEntity.DepivotedPurchaseHistory, "M1").gt(1).build();
        Restriction m2 = Restriction.builder().let(BusinessEntity.DepivotedPurchaseHistory, "M2").gt(2).build();
        Restriction m3 = Restriction.builder().let(BusinessEntity.DepivotedPurchaseHistory, "M3").gt(3).build();

        Restriction r1 = m1;
        Restriction e1 = new MetricRestriction(BusinessEntity.DepivotedPurchaseHistory, r1);

        Restriction r2 = Restriction.builder().or(m1, m2).build();
        Restriction e2 = new MetricRestriction(BusinessEntity.DepivotedPurchaseHistory, //
                JsonUtils.deserialize(JsonUtils.serialize(r2), Restriction.class));

        Restriction r31 = Restriction.builder().and(m1, m2).build();
        Restriction r3 = Restriction.builder().and(r31, m3).build();
        Restriction e31 = Restriction.builder().and(m1, m2, m3).build();
        Restriction e3 = new MetricRestriction(BusinessEntity.DepivotedPurchaseHistory, e31);

        Restriction r41 = Restriction.builder().and(m1, m2, A1).build();
        Restriction r4 = Restriction.builder().and(r41, m3).build();
        Restriction e41 = new MetricRestriction(BusinessEntity.DepivotedPurchaseHistory, Restriction.builder().and(m1, m2, m3).build());
        Restriction e4 = Restriction.builder().and(A1, e41).build();

        return new Object[][] { //
                 { r1, e1 }, //
                 { r2, e2 }, //
                 { r3, e3 }, //
                 { r4, e4 } //
        };
    }

    private static BucketRestriction bucket(BusinessEntity entity, int idx) {
        Bucket bucket = Bucket.valueBkt(String.valueOf(idx));
        AttributeLookup attrLookup = new AttributeLookup(entity, entity.name().substring(0, 1));
        BucketRestriction bucketRestriction = new BucketRestriction(attrLookup, bucket);
        if (BusinessEntity.Contact.equals(entity) && idx == 5) {
            bucketRestriction.setIgnored(true);
        }
        return bucketRestriction;
    }

    private static BucketRestriction listBucket(int idx) {
        ComparisonType operator = (idx <= 2) ? IN_COLLECTION : ComparisonType.NOT_IN_COLLECTION;
        List<Object> vals = Arrays.asList(String.valueOf(idx), String.valueOf(idx * 10), String.valueOf(idx * 100));
        Bucket bucket = Bucket.valueBkt(operator, vals);
        BusinessEntity entity = Account;
        AttributeLookup attrLookup = new AttributeLookup(entity, entity.name().substring(0, 1));
        return new BucketRestriction(attrLookup, bucket);
    }

    private static BucketRestriction listBucket(boolean in, Object... vals) {
        ComparisonType operator = in ? IN_COLLECTION : ComparisonType.NOT_IN_COLLECTION;
        Bucket bucket = Bucket.valueBkt(operator, Arrays.asList(vals));
        BusinessEntity entity = Account;
        AttributeLookup attrLookup = new AttributeLookup(entity, entity.name().substring(0, 1));
        return new BucketRestriction(attrLookup, bucket);
    }

    private static Restriction and(Restriction... restrictions) {
        return Restriction.builder().and(restrictions).build();
    }

    private static Restriction or(Restriction... restrictions) {
        return Restriction.builder().or(restrictions).build();
    }

}
