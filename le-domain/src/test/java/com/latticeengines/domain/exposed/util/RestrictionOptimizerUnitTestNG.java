package com.latticeengines.domain.exposed.util;

import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
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
    private static final Restriction C5 = bucket(Contact, 5);

    @Test(groups = "unit", dataProvider = "flattenTestData")
    public void testFlatten(Restriction restriction, Restriction expected) {
        Restriction flatten = RestrictionOptimizer.flatten(restriction);
        Assert.assertEquals(JsonUtils.serialize(flatten), JsonUtils.serialize(expected));
    }

    @DataProvider(name = "flattenTestData", parallel = true)
    public Object[][] provideFlattenTestData() {
        Restriction r1 = and(A1, and(A2), and(A3, A4));
        Restriction e1 = and(A1, A2, A3, A4);

        Restriction r2 = or(and(A1, A2), and(A3, A4));
        Restriction e2 = or(and(A1, A2), and(A3, A4));

        Restriction r3 = or(or(C1, C2), and(A3, A4), A5);
        Restriction e3 = or(C1, C2, and(A3, A4), A5);

        Restriction r4 = and(C1, or(A2), and(C3, or(C4, A5)));
        Restriction e4 = and(C1, A2, C3, or(C4, A5));

        return new Object[][] { //
                { r1, e1 }, //
                { r2, e2 }, //
                { r3, e3 }, //
                { r4, e4 }, //
        };
    }

    @Test(groups = "unit", dataProvider = "groupTestData")
    public void testGroup(Restriction restriction, Restriction expected) {
        Restriction grouped = RestrictionOptimizer.group(restriction);
        Assert.assertEquals(JsonUtils.serialize(grouped), JsonUtils.serialize(expected));
    }

    @DataProvider(name = "groupTestData", parallel = true)
    public Object[][] provideGroupTestData() {
        Restriction r1 = and(A1, C1, A2, C2);
        Restriction e1 = and(and(A1, A2), and(C1, C2));

        Restriction r2 = or(and(A1, A2, C1), or(A1, C2, C3));
        Restriction e2 = or(and(and(A1, A2), C1), or(A1, or(C2, C3)));

        return new Object[][] { //
                { r1, e1 }, //
                { r2, e2 }, //
        };
    }

    @Test(groups = "unit", dataProvider = "optimizeTestData")
    public void testOptimize(Restriction restriction, Restriction expected) {
        Restriction optimized = RestrictionOptimizer.optimize(restriction);
        Assert.assertEquals(JsonUtils.serialize(optimized), JsonUtils.serialize(expected));
    }

    @DataProvider(name = "optimizeTestData", parallel = true)
    public Object[][] provideOptimizeTestData() {
        Restriction r1 = and(A1, C1, A2, C2);
        Restriction e1 = and(and(A1, A2), and(C1, C2));

        Restriction r2 = or(and(A1, A2, C1), or(A1, C2, C3));
        Restriction e2 = or(and(and(A1, A2), C1), A1, or(C2, C3));

        Restriction r3 = and(C1, or(A2), and(C3, or(C4, A5)));
        Restriction e3 = and(or(A5, C4), A2, and(C1, C3));

        return new Object[][] { //
                { r1, e1 }, //
                { r2, e2 }, //
                { r3, e3 }, //
        };
    }

    @Test(groups = "unit", dataProvider = "nullTestData")
    public void testNull(Restriction restriction) {
        Assert.assertNull(RestrictionOptimizer.optimize(restriction));
    }

    @DataProvider(name = "nullTestData", parallel = true)
    public Object[][] provideNullTestData() {
        return new Object[][] { //
                { null }, //
                { and() }, //
                { or() }, //
                { and(and(), or()) }, //
        };
    }

    private static BucketRestriction bucket(BusinessEntity entity, int idx) {
        return BucketRestriction.from(Restriction.builder() //
                .let(entity, entity.name().substring(0, 1)) //
                .eq(String.valueOf(idx)) //
                .build());
    }

    private static Restriction and(Restriction... restrictions) {
        return Restriction.builder().and(restrictions).build();
    }

    private static Restriction or(Restriction... restrictions) {
        return Restriction.builder().or(restrictions).build();
    }

}
