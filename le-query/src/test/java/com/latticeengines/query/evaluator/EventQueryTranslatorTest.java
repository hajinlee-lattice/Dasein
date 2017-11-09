package com.latticeengines.query.evaluator;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AggregationSelector;
import com.latticeengines.domain.exposed.query.AggregationType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.query.exposed.translator.EventQueryTranslator;
import com.latticeengines.query.exposed.translator.TransactionRestrictionTranslator;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;
import com.querydsl.sql.SQLQuery;

public class EventQueryTranslatorTest extends QueryFunctionalTestNGBase {

    public static class EnhancedEventQueryTranslator extends EventQueryTranslator {
        @Override
        protected String getTransactionTableName(AttributeRepository repository) {
            return "tftest_4_transaction_2017_10_31_19_44_08_utc";
        }
    }

    private EventQueryTranslator getEventQueryTranslator() {
        return new EnhancedEventQueryTranslator();
    }

    private TransactionRestriction getHasEngaged() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId("A78DF03BAC196BE9A08508FFDB433A31");
        txRestriction.setTimeFilter(TimeFilter.ever());
        return txRestriction;
    }

    private TransactionRestriction getSumAmount() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        // 3872223C9BA06C649D68E415E23A9446
        txRestriction.setProductId("A78DF03BAC196BE9A08508FFDB433A31");
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.SPENT,
                AggregationType.SUM,
                ComparisonType.GREATER_THAN,
                Arrays.asList(5000)
        );
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getSumQuantity() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        // 3872223C9BA06C649D68E415E23A9446
        txRestriction.setProductId("A78DF03BAC196BE9A08508FFDB433A31");
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.UNIT,
                AggregationType.SUM,
                ComparisonType.LESS_THAN,
                Arrays.asList(20)
        );
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getAtLeastOnceAmount() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        // 3872223C9BA06C649D68E415E23A9446
        txRestriction.setProductId("A78DF03BAC196BE9A08508FFDB433A31");
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.SPENT,
                AggregationType.AT_LEAST_ONCE,
                ComparisonType.GREATER_THAN,
                Arrays.asList(3000)
        );
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    @Test(groups = "functional")
    public void testLogicalOrOneChild() {
        TransactionRestriction txRestriction = getHasEngaged();
        Restriction l = Restriction.builder().or(txRestriction).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, l);
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        //System.out.println("sqlQuery = " + sqlQuery);
    }

    @Test(groups = "functional")
    public void testLogicalAndOneChild() {
        TransactionRestriction txRestriction = getHasEngaged();
        Restriction l = Restriction.builder().and(txRestriction).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, l);
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        //System.out.println("sqlQuery = " + sqlQuery);
    }

    @Test(groups = "functional")
    public void testLogicalAndTwoChildren() {
        TransactionRestriction t1 = getHasEngaged();
        TransactionRestriction t2 = getSumAmount();
        Restriction l1 = Restriction.builder().and(t1, t2).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, l1);
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        //System.out.println("sqlQuery = " + sqlQuery);
    }

    @Test(groups = "functional")
    public void testLogicalWithTwoLevels() {
        TransactionRestriction t1 = getHasEngaged();
        TransactionRestriction t2 = getSumAmount();
        TransactionRestriction t3 = getHasEngaged();
        TransactionRestriction t4 = getSumAmount();
        Restriction l1 = Restriction.builder().and(t1, t2).build();
        Restriction l2 = Restriction.builder().and(t3, t4, l1).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, l2);
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        //System.out.println("sqlQuery = " + sqlQuery);
    }

    @Test(groups = "functional")
    public void testLogicalWithTwoFullLevels() {
        TransactionRestriction t1 = getHasEngaged();
        TransactionRestriction t2 = getSumAmount();
        TransactionRestriction t3 = getHasEngaged();
        TransactionRestriction t4 = getSumAmount();
        Restriction l1 = Restriction.builder().or(t1, t2).build();
        Restriction l2 = Restriction.builder().or(t3, t4).build();
        Restriction l3 = Restriction.builder().and(l1, l2).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, l3);
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        //System.out.println("sqlQuery = " + sqlQuery);
    }

    @Test(groups = "functional")
    public void testLogicalOrTwoChildren() {
        TransactionRestriction t1 = getHasEngaged();
        TransactionRestriction t2 = getSumAmount();
        Restriction l1 = Restriction.builder().or(t1, t2).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, l1);
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        //System.out.println("sqlQuery = " + sqlQuery);
    }

    @Test(groups = "functional")
    public void testHasEngaged() {
        // has engaged
        TransactionRestriction txRestriction = getHasEngaged();
        // 3872223C9BA06C649D68E415E23A9446
        // A78DF03BAC196BE9A08508FFDB433A31
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction);
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        //System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query);
        Assert.assertEquals(count, 21199);
    }


    @Test(groups = "functional")
    public void testSumAmount() {
        TransactionRestriction txRestriction = getSumAmount();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction);
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        //System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query);
        Assert.assertEquals(count, 1508);

    }

    @Test(groups = "functional")
    public void testSumQuantity() {
        TransactionRestriction txRestriction = getSumQuantity();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction);
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        //System.out.println("sqlQuery = " + sqlQuery);

    }

    @Test(groups = "functional")
    public void testAtLeastOnceAmount() {
        TransactionRestriction txRestriction = getAtLeastOnceAmount();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction);
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        //System.out.println("sqlQuery = " + sqlQuery);

    }

    @Test(groups = "functional")
    public void testSimpleAccountQuery() {
        Restriction acctRestriction = Restriction.builder()
                .let(BusinessEntity.Account, ATTR_ACCOUNT_ID).eq("0012400001DNKKLAA5").build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, acctRestriction);
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        //System.out.println("sqlQuery = " + sqlQuery);

    }

}
