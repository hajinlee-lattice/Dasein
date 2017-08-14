package com.latticeengines.objectapi.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.commons.lang3.mutable.MutableInt;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.graph.traversal.impl.BreadthFirstSearch;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.LogicalOperator;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;

public class QueryTranslatorUnitTestNG {

    @Test(groups = "unit")
    public void testTranslate() {
        FrontEndQuery query = new FrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        frontEndRestriction.setRestriction(createRestriction(Level.Simple));
        query.setFrontEndRestriction(frontEndRestriction);

        Query translated = QueryTranslator.translate(query, AccountQueryDecorator.WITHOUT_SELECTS);
        assertTrue(translated.getRestriction() instanceof LogicalRestriction);
        LogicalRestriction parent = (LogicalRestriction) translated.getRestriction();
        assertEquals(parent.getRestrictions().size(), 5);
        assertTrue(parent.getRestrictions().stream()
                .anyMatch(r -> ((LogicalRestriction) r).getOperator().equals(LogicalOperator.AND)));
        assertTrue(parent.getRestrictions().stream()
                .anyMatch(r -> ((LogicalRestriction) r).getOperator().equals(LogicalOperator.OR)));

        validateTranslated(translated.getRestriction(), 4, 7);
    }

    private void validateTranslated(Restriction restriction, int numConcrete, int numTotalRestrictions) {
        BreadthFirstSearch search = new BreadthFirstSearch();
        final MutableInt concreteCounter = new MutableInt(0);
        final MutableInt totalCounter = new MutableInt(0);
        search.run(restriction, (object, ctx) -> {
            if (object instanceof Restriction) {
                totalCounter.increment();
            }
            assertFalse(object instanceof BucketRestriction);
            if (object instanceof ConcreteRestriction) {
                concreteCounter.increment();
            }
        });
        assertEquals(numConcrete, concreteCounter.intValue());
        assertEquals(numTotalRestrictions, totalCounter.intValue());
    }

    @Test(groups = "unit")
    public void testTranslateWithDecorator() {
        FrontEndQuery query = new FrontEndQuery();
        query.setFreeFormTextSearch("intel");
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        frontEndRestriction.setRestriction(createRestriction(Level.Simple));
        query.setFrontEndRestriction(frontEndRestriction);

        Query result = QueryTranslator.translate(query, AccountQueryDecorator.WITH_SELECTS);
        assertTrue(result.getLookups().size() > 0);
        assertTrue(result.getFreeFormTextSearchAttributes().size() > 0);
    }

    private enum Level {
        Simple, Advanced
    }

    private Restriction createRestriction(Level level) {
        BucketRestriction a = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"),
                Bucket.rangeBkt(2, 3));

        BucketRestriction b = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "B"),
                Bucket.rangeBkt(200, 300));

        BucketRestriction c = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "C"),
                Bucket.valueBkt("Yes"));

        BucketRestriction d = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "D"),
                Bucket.valueBkt("No"));

        BucketRestriction e = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "E"),
                Bucket.rangeBkt(10, 100));

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
        frontEndQuery.setFreeFormTextSearch("Boulder");
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        frontEndQuery.setFrontEndRestriction(frontEndRestriction);

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

}
