package com.latticeengines.objectapi.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.commons.lang3.mutable.MutableInt;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.graph.traversal.impl.BreadthFirstSearch;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.ExistsRestriction;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;

public class QueryTranslatorUnitTestNG {

    @Test(groups = "unit")
    public void testTranslate() {
        FrontEndQuery query = new FrontEndQuery();
        FrontEndRestriction accountRestriction = new FrontEndRestriction();
        accountRestriction.setRestriction(createAccountRestriction(Level.Simple));
        query.setAccountRestriction(accountRestriction);
        query.setMainEntity(BusinessEntity.Account);

        RatingQueryTranslator queryTranslator = new RatingQueryTranslator(null, null);
        Query translated = queryTranslator.translateRatingQuery(query, false, null, "user");
        assertTrue(translated.getRestriction() instanceof LogicalRestriction);
        validateTranslated(translated.getRestriction(), 6, 8, 0);
        validateTranslatedLookups(translated, 0);
    }

    @Test(groups = "unit")
    public void testContactTranslate() {
        FrontEndQuery query = new FrontEndQuery();
        FrontEndRestriction contactRestriction = new FrontEndRestriction();
        contactRestriction.setRestriction(createContactRestriction(Level.Simple));
        query.setContactRestriction(contactRestriction);
        query.setMainEntity(BusinessEntity.Contact);

        RatingQueryTranslator queryTranslator = new RatingQueryTranslator(null, null);
        Query translated = queryTranslator.translateRatingQuery(query, false, null, "user");
        assertTrue(translated.getRestriction() instanceof LogicalRestriction);
        validateTranslated(translated.getRestriction(), 4, 6, 0);
        validateTranslatedLookups(translated, 0);
    }

    @Test(groups = "unit")
    public void testAccountAndContactTranslate() {
        FrontEndQuery query = new FrontEndQuery();
        FrontEndRestriction accountRestriction = new FrontEndRestriction();
        accountRestriction.setRestriction(createAccountRestriction(Level.Simple));
        query.setAccountRestriction(accountRestriction);
        FrontEndRestriction contactRestriction = new FrontEndRestriction();
        contactRestriction.setRestriction(createContactRestriction(Level.Simple));
        query.setContactRestriction(contactRestriction);
        query.setMainEntity(BusinessEntity.Account);

        RatingQueryTranslator queryTranslator = new RatingQueryTranslator(null, null);
        Query translated = queryTranslator.translateRatingQuery(query, false, null, "user");
        assertTrue(translated.getRestriction() instanceof LogicalRestriction);
        validateTranslated(translated.getRestriction(), 7, 10, 0);
    }

    @Test(groups = "unit")
    public void testContactAndAccountTranslate() {
        FrontEndQuery query = new FrontEndQuery();
        FrontEndRestriction accountRestriction = new FrontEndRestriction();
        accountRestriction.setRestriction(createAccountRestriction(Level.Simple));
        query.setAccountRestriction(accountRestriction);
        FrontEndRestriction contactRestriction = new FrontEndRestriction();
        contactRestriction.setRestriction(createContactRestriction(Level.Simple));
        query.setContactRestriction(contactRestriction);
        query.setMainEntity(BusinessEntity.Contact);

        RatingQueryTranslator queryTranslator = new RatingQueryTranslator(null, null);
        Query translated = queryTranslator.translateRatingQuery(query, true, null, "user");
        assertTrue(translated.getRestriction() instanceof LogicalRestriction);
        validateTranslated(translated.getRestriction(), 5, 8, 0);
        validateTranslatedLookups(translated, 0);
    }

    private void validateTranslatedLookups(Query translated, int expectedLookups) {
        assertEquals(translated.getLookups().size(), expectedLookups);
    }

    private void validateTranslated(Restriction restriction, int numConcrete, int numTotalRestrictions, int numExists) {
        BreadthFirstSearch search = new BreadthFirstSearch();
        final MutableInt concreteCounter = new MutableInt(0);
        final MutableInt totalCounter = new MutableInt(0);
        final MutableInt existCounter = new MutableInt(0);
        search.run(restriction, (object, ctx) -> {
            if (object instanceof Restriction) {
                totalCounter.increment();
            }
            assertFalse(object instanceof BucketRestriction);
            if (object instanceof ConcreteRestriction) {
                concreteCounter.increment();
            }
            if (object instanceof ExistsRestriction) {
                existCounter.increment();
            }
        });
        assertEquals(concreteCounter.intValue(), numConcrete);
        assertEquals(totalCounter.intValue(), numTotalRestrictions);
        assertEquals(existCounter.intValue(), numExists);
    }

    private enum Level {
        Simple, Advanced
    }

    private Restriction createAccountRestriction(Level level) {
        BucketRestriction a = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"),
                Bucket.rangeBkt(2, 3));

        BucketRestriction b = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "B"),
                Bucket.rangeBkt(200, 300));

        BucketRestriction c = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "C"),
                Bucket.valueBkt("Yes"));

        BucketRestriction d = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "D"),
                Bucket.valueBkt("No"));

        BucketRestriction e = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "E"),
                Bucket.dateBkt(TimeFilter.latestDay()));

        if (level == Level.Simple) {
            Restriction and = Restriction.builder().and(a, b).build();
            Restriction or = Restriction.builder().or(c, d).build();
            return Restriction.builder().and(and, or).build();
        } else if (level == Level.Advanced) {
            // AND1 (OR1 (AND2(OR2(A,B)), E), C)) D))
            Restriction or2 = Restriction.builder().or(a, b).build();
            Restriction and2 = Restriction.builder().and(or2, e).build();
            Restriction or1 = Restriction.builder().or(and2, c).build();
            Restriction and1 = Restriction.builder().and(or1, d).build();

            return and1;
        }

        return null;
    }

    private Restriction createContactRestriction(Level level) {
        Restriction a = Restriction.builder().let(BusinessEntity.Contact, "A").eq(1).build();
        Restriction b = Restriction.builder().let(BusinessEntity.Contact, "B").eq(2).build();
        Restriction c = Restriction.builder().let(BusinessEntity.Contact, "C").eq(3).build();
        Restriction d = Restriction.builder().let(BusinessEntity.Contact, "D").eq(4).build();
        Restriction e = Restriction.builder().let(BusinessEntity.Contact, "E").eq(5).build();

        if (level == Level.Simple) {
            Restriction and = Restriction.builder().and(a, b).build();
            Restriction or = Restriction.builder().or(c, d).build();
            return Restriction.builder().and(and, or).build();
        } else if (level == Level.Advanced) {
            // AND1 (OR1 (AND2(OR2(A,B)), E), C)) D))
            Restriction or2 = Restriction.builder().or(a, b).build();
            Restriction and2 = Restriction.builder().and(or2, e).build();
            Restriction or1 = Restriction.builder().or(and2, c).build();
            Restriction and1 = Restriction.builder().and(or1, d).build();

            return and1;
        }

        return null;
    }

    @Test(groups = "unit")
    public void testGenerateSampleQueries() {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        frontEndQuery.setAccountRestriction(frontEndRestriction);

        BucketRestriction a = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "PD_DC_FEATURETERMCREATEAFREEWEB_E93595C307"),
                Bucket.rangeBkt(0, 1));
        BucketRestriction b = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "ACCT_I_RANK_PCTCHANGE_6MONTH_470ECBCC2A"),
                Bucket.rangeBkt(10, 20));
        BucketRestriction c = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "PD_DC_FEATURETERMCHECKOUT_16ED68E665"),
                Bucket.rangeBkt(0, 5));
        BucketRestriction d = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "PD_DC_FEATURETERMCHECKOUT_16ED68E665"),
                Bucket.rangeBkt(5, 25));
        BucketRestriction e = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "PD_DC_FEATURETERMCREATEAFREEWEB_E93595C307"),
                Bucket.rangeBkt(1, 10));

        Restriction and = Restriction.builder().and(a, b).build();
        Restriction or = Restriction.builder().or(c, d).build();
        Restriction restriction = Restriction.builder().and(and, or).build();
        frontEndRestriction.setRestriction(restriction);

        System.out.println(JsonUtils.serialize(frontEndRestriction));
        System.out.println(JsonUtils.serialize(frontEndQuery));

        // AND1 (OR1 (AND2(OR2(C,D)), E), A)) B))
        Restriction or2 = Restriction.builder().or(c, d).build();
        Restriction and2 = Restriction.builder().and(or2, e).build();
        Restriction or1 = Restriction.builder().or(and2, a).build();
        Restriction and1 = Restriction.builder().and(or1, b).build();
        frontEndRestriction.setRestriction(and1);
        System.out.println(JsonUtils.serialize(frontEndRestriction));
    }

    @Test(groups = "unit")
    public void testDeletedRestrictions() {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        frontEndQuery.setAccountRestriction(frontEndRestriction);

        BucketRestriction a = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "PD_DC_FEATURETERMCREATEAFREEWEB_E93595C307"),
                Bucket.rangeBkt(0, 1));
        BucketRestriction b = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "ACCT_I_RANK_PCTCHANGE_6MONTH_470ECBCC2A"),
                Bucket.rangeBkt(10, 20));
        BucketRestriction c = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "PD_DC_FEATURETERMCHECKOUT_16ED68E665"),
                Bucket.rangeBkt(0, 5));
        BucketRestriction d = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "PD_DC_FEATURETERMCHECKOUT_16ED68E665"),
                Bucket.rangeBkt(5, 25));
        BucketRestriction e = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "PD_DC_FEATURETERMCREATEAFREEWEB_E93595C308"),
                Bucket.rangeBkt(1, 10));

        b.setIgnored(true);
        e.setIgnored(true);

        Restriction and = Restriction.builder().and(a, b).build();
        Restriction or = Restriction.builder().or(c, d).build();
        Restriction restriction = Restriction.builder().and(and, or).build();
        frontEndRestriction.setRestriction(restriction);

        RatingQueryTranslator queryTranslator = new RatingQueryTranslator(null, null);
        Query result = queryTranslator.translateRatingQuery(frontEndQuery, true, null, "user");

        // AND1 (OR1 (AND2(OR2(C,D))), A))))
        Restriction translated = result.getRestriction();
        System.out.println(translated.toString());
        Assert.assertFalse(translated.toString().contains("ACCT_I_RANK_PCTCHANGE_6MONTH_470ECBCC2A"));
        Assert.assertFalse(translated.toString().contains("PD_DC_FEATURETERMCREATEAFREEWEB_E93595C308"));

    }

}
