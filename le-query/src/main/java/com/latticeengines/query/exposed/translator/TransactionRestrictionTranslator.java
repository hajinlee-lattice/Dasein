package com.latticeengines.query.exposed.translator;

import static com.latticeengines.domain.exposed.query.AggregationType.AT_LEAST_ONCE;
import static com.latticeengines.domain.exposed.query.AggregationType.EACH;

import java.math.BigDecimal;

import org.apache.commons.lang.RandomStringUtils;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.FunctionLookup;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.TimeFilter.Period;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.domain.exposed.query.ValueLookup;
import com.latticeengines.domain.exposed.query.util.ExpressionTemplateUtils;
import com.latticeengines.domain.exposed.util.RestrictionUtils;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.Expressions;

public class TransactionRestrictionTranslator {
    private TransactionRestriction txnRestriction;

    public final static String PERIOD_AMOUNT = "PeriodAmount";
    public final static String PERIOD_QUANTITY = "PeriodQuantity";
    public final static String PERIOD_OFFSET = "PeriodOffset";
    public final static String MAX_PERIOD_OFFSET = "MaxPeriodOffset";
    public final static String PERIOD_COUNT = "PeriodCount";

    public TransactionRestrictionTranslator(TransactionRestriction txnRestriction) {
        this.txnRestriction = txnRestriction;
    }

    public Restriction convert(BusinessEntity entity) {
        if (txnRestriction.getTimeFilter() == null) {
            txnRestriction.setTimeFilter(TimeFilter.ever());
        }

        // treat PRIOR_TO_LAST specially to match playmaker functionality
        if (ComparisonType.PRIOR_TO_LAST == txnRestriction.getTimeFilter().getRelation()) {

            if (txnRestriction.isNegate()) {
                Restriction notPriorRestriction = translateToPrior(entity, txnRestriction, true);
                Restriction withinRestriction = translateToHasNotPurchasedWithin(entity, txnRestriction, true);
                return Restriction.builder().or(notPriorRestriction, withinRestriction).build();
            } else {
                Restriction priorRestriction = translateToPrior(entity, txnRestriction, false);
                Restriction notWithinRestriction = translateToHasNotPurchasedWithin(entity, txnRestriction, false);
                return Restriction.builder().and(priorRestriction, notWithinRestriction).build();
            }

        } else {
            return translate(entity);
        }
    }

    private FunctionLookup<Integer, Object, String> createPeriodOffsetLookup() {
        AttributeLookup transactionDate = new AttributeLookup(BusinessEntity.Transaction,
                InterfaceName.TransactionDate.name());
        AttributeLookup periodOffset = AttributeLookup.fromString(PERIOD_OFFSET);

        Period p = txnRestriction.getTimeFilter().getPeriod();
        String source = ExpressionTemplateUtils.strAttrToDate(transactionDate.toString());
        String target = ExpressionTemplateUtils.getCurrentDate();
        FunctionLookup<Integer, Object, String> period = new FunctionLookup<Integer, Object, String>(Integer.class,
                periodOffset).as(PERIOD_OFFSET);
        period.setFunction(args -> ExpressionTemplateUtils.getDateDiffTemplate(p, source, target));
        return period;
    }

    private FunctionLookup<Integer, Object, String> createMaxPeriodOffsetLookup() {
        AttributeLookup transactionDate = new AttributeLookup(BusinessEntity.Transaction,
                InterfaceName.TransactionDate.name());
        AttributeLookup periodOffset = AttributeLookup.fromString(PERIOD_OFFSET);

        Period p = txnRestriction.getTimeFilter().getPeriod();
        String source = ExpressionTemplateUtils.strAttrToDate(transactionDate.toString());
        String target = ExpressionTemplateUtils.getCurrentDate();
        FunctionLookup<Integer, Object, String> period = new FunctionLookup<Integer, Object, String>(Integer.class,
                periodOffset).as(MAX_PERIOD_OFFSET);
        period.setFunction(args -> ExpressionTemplateUtils.getMaxDateDiffTemplate(p, source, target));

        return period;
    }

    private Restriction translateToPrior(BusinessEntity businessEntity, TransactionRestriction res, boolean negate) {

        TimeFilter timeFilter = new TimeFilter(res.getTimeFilter().getLhs(), ComparisonType.PRIOR, //
                res.getTimeFilter().getPeriod(), res.getTimeFilter().getValues());
        TransactionRestriction prior = new TransactionRestriction(res.getProductName(), //
                res.getProductId(), //
                timeFilter, //
                negate, //
                res.getSpentFilter(), //
                res.getUnitFilter());

        return new TransactionRestrictionTranslator(prior).translate(businessEntity);
    }

    private Restriction translateToHasNotPurchasedWithin(BusinessEntity businessEntity, TransactionRestriction res, //
            boolean negate) {

        TimeFilter timeFilter = new TimeFilter(res.getTimeFilter().getLhs(), ComparisonType.WITHIN, //
                res.getTimeFilter().getPeriod(), res.getTimeFilter().getValues());

        TransactionRestriction notWithin = new TransactionRestriction(res.getProductName(), //
                res.getProductId(), //
                timeFilter, //
                !negate, //
                null, //
                null);

        return new TransactionRestrictionTranslator(notWithin).translate(businessEntity);
    }

    private Restriction translate(BusinessEntity entity) {

        Restriction productRestriction = filterByProduct();

        AttributeLookup amountLookup = new AttributeLookup(BusinessEntity.Transaction,
                InterfaceName.TotalAmount.name());
        AttributeLookup quantityLookup = new AttributeLookup(BusinessEntity.Transaction,
                InterfaceName.TotalQuantity.name());
        AttributeLookup accountId = new AttributeLookup(BusinessEntity.Transaction, InterfaceName.AccountId.name());
        FunctionLookup<Integer, Object, String> period = createPeriodOffsetLookup();

        Query innerQuery = Query.builder().from(BusinessEntity.Transaction)
                .select(accountId, period, amountLookup, quantityLookup).where(productRestriction).build();
        SubQuery innerSubQuery = new SubQuery(innerQuery, generateAlias(BusinessEntity.Transaction));

        SubQueryAttrLookup subQueryAccountId = new SubQueryAttrLookup(innerSubQuery, InterfaceName.AccountId.name());
        SubQueryAttrLookup subQueryPeriodOffset = new SubQueryAttrLookup(innerSubQuery, PERIOD_OFFSET);
        SubQueryAttrLookup subQueryPeriodAmount = new SubQueryAttrLookup(innerSubQuery,
                InterfaceName.TotalAmount.name());
        SubQueryAttrLookup subQueryPeriodQuantity = new SubQueryAttrLookup(innerSubQuery,
                InterfaceName.TotalQuantity.name());

        AggregateLookup periodAmount = AggregateLookup.sum(subQueryPeriodAmount).as(PERIOD_AMOUNT);
        AggregateLookup periodQuantity = AggregateLookup.sum(subQueryPeriodQuantity).as(PERIOD_QUANTITY);

        Restriction timeRestriction = filterByTime(subQueryPeriodOffset);
        Query middleQuery = Query.builder().from(innerSubQuery)
                .select(subQueryAccountId, subQueryPeriodOffset, periodAmount, periodQuantity).where(timeRestriction)
                .groupBy(groupByAccountAndPeriod(subQueryAccountId, subQueryPeriodOffset))
                .having(filterByPeriodAmountQuantity(periodAmount, periodQuantity)).build();

        SubQuery middleSubQuery = new SubQuery(middleQuery, generateAlias(BusinessEntity.Transaction));

        subQueryAccountId = new SubQueryAttrLookup(middleSubQuery, InterfaceName.AccountId.name());
        SubQueryAttrLookup periodTotalAmountLookup = new SubQueryAttrLookup(middleSubQuery, PERIOD_AMOUNT);
        SubQueryAttrLookup periodTotalQuantityLookup = new SubQueryAttrLookup(middleSubQuery, PERIOD_QUANTITY);

        Query outerQuery = Query.builder().from(middleSubQuery).select(subQueryAccountId)
                .groupBy(subQueryAccountId) //
                .having(filterByAggregatedPeriodAmountQuantity(periodTotalAmountLookup, periodTotalQuantityLookup))
                .build();

        SubQuery txSubQuery = new SubQuery(outerQuery, generateAlias(BusinessEntity.Transaction));
        ConcreteRestriction accountInRestriction = (ConcreteRestriction) Restriction.builder()
                .let(entity, InterfaceName.AccountId.name()).inCollection(txSubQuery, InterfaceName.AccountId.name())
                .build();

        return txnRestriction.isNegate() ? Restriction.builder().not(accountInRestriction).build()
                : accountInRestriction;
    }

    private Lookup[] groupByAccountAndPeriod(Lookup accountId, Lookup period) {
        return new Lookup[] { accountId, period };
    }

    private Restriction filterByPeriodAmountQuantity(AggregateLookup aggrAmount, AggregateLookup aggrQuantity) {
        Restriction restriction = null;
        AggregationFilter spentFilter = txnRestriction.getSpentFilter();
        AggregationFilter unitFilter = txnRestriction.getUnitFilter();
        if (spentFilter != null && unitFilter != null) {
            Restriction amountRestriction = filterByAggregationType(aggrAmount, spentFilter);
            Restriction quantityRestriction = filterByAggregationType(aggrQuantity, unitFilter);
            restriction = Restriction.builder().and(amountRestriction, quantityRestriction).build();
        } else if (spentFilter != null) {
            restriction = filterByAggregationType(aggrAmount, spentFilter);
        } else if (unitFilter != null) {
            restriction = filterByAggregationType(aggrQuantity, unitFilter);
        }
        return restriction;
    }

    private Restriction filterByAggregationType(Lookup lookup, AggregationFilter filter) {
        Restriction restriction = null;
        if (EACH == filter.getAggregationType() || AT_LEAST_ONCE == filter.getAggregationType()) {
            restriction = RestrictionUtils.convertValueComparisons(lookup, filter.getComparisonType(),
                    filter.getValues());
        }
        return restriction;
    }

    private Restriction filterByAggregatedPeriodAmountQuantity(Lookup periodAmountLookup, Lookup periodQuantityLookup) {
        AggregationFilter spentFilter = txnRestriction.getSpentFilter();
        AggregationFilter unitFilter = txnRestriction.getUnitFilter();
        Restriction restriction;
        if (spentFilter == null && unitFilter == null) {
            // has purchased, treat it as sum(amount) > 0 or sum(unit) > 0
            AggregateLookup aggrAmount = AggregateLookup.sum(periodAmountLookup);
            AggregateLookup aggrQuantity = AggregateLookup.sum(periodQuantityLookup);
            Restriction amountRestriction = Restriction.builder().let(aggrAmount).gt(0).build();
            Restriction quantityRestriction = Restriction.builder().let(aggrQuantity).gt(0).build();
            restriction = Restriction.builder().or(amountRestriction, quantityRestriction).build();
        } else if (spentFilter != null && unitFilter == null) {
            restriction = getAggregatedRestriction(periodAmountLookup, spentFilter);
        } else if (spentFilter == null && unitFilter != null) {
            restriction = getAggregatedRestriction(periodQuantityLookup, unitFilter);
        } else {
            Restriction amountRestriction = getAggregatedRestriction(periodAmountLookup, spentFilter);
            Restriction quantityRestriction = getAggregatedRestriction(periodQuantityLookup, unitFilter);
            restriction = Restriction.builder().and(amountRestriction, quantityRestriction).build();
        }
        return restriction;
    }

    private Restriction getAggregatedRestriction(Lookup mixin, AggregationFilter filter) {
        Restriction restriction = null;
        switch (filter.getAggregationType()) {
        case AVG:
            restriction = RestrictionUtils.convertValueComparisons(AggregateLookup.avg(mixin),
                    filter.getComparisonType(), filter.getValues());
            break;
        case SUM:
            restriction = RestrictionUtils.convertValueComparisons(AggregateLookup.sum(mixin),
                    filter.getComparisonType(), filter.getValues());
            break;
        case AT_LEAST_ONCE:
            restriction = Restriction.builder().let(AggregateLookup.count()).gt(0).build();
            break;
        case EACH:
            restriction = Restriction.builder().let(AggregateLookup.count()).eq(createPeriodCountLookup())
                    .build();
            break;
        default:
            throw new UnsupportedOperationException("Unsupported aggregation type " + filter.getAggregationType());
        }
        return restriction;
    }

    private Lookup createPeriodCountLookup() {
        Lookup lookup;
        switch (txnRestriction.getTimeFilter().getRelation()) {
        case EVER:
            FunctionLookup<Integer, Object, String> everMax = createMaxPeriodOffsetLookup();
            FunctionLookup<Integer, Expression<BigDecimal>, Expression<BigDecimal>> everCount = new FunctionLookup<>(
                    Integer.class, everMax);
            everCount.setFunction(maxOffset -> Expressions.asNumber(maxOffset).add(1));
            everCount.setAlias(PERIOD_COUNT);
            Query everCountQuery = Query.builder().from(BusinessEntity.Transaction).select(everCount).build();
            SubQuery everCountSubQuery = new SubQuery(everCountQuery, generateAlias(BusinessEntity.Transaction));
            lookup = new SubQueryAttrLookup(everCountSubQuery, PERIOD_COUNT);
            break;
        case IN_CURRENT_PERIOD:
            lookup = new ValueLookup(1);
            break;
        case EQUAL:
            Integer offset = Integer.valueOf(txnRestriction.getTimeFilter().getValues().get(0).toString());
            lookup = new ValueLookup(offset);
            break;
        case WITHIN:
            Integer withinMax = Integer.valueOf(txnRestriction.getTimeFilter().getValues().get(0).toString());
            lookup = new ValueLookup(withinMax + 1);
            break;
        case PRIOR:
            Integer prior = Integer.valueOf(txnRestriction.getTimeFilter().getValues().get(0).toString());
            FunctionLookup<Integer, Object, String> offsetMax = createMaxPeriodOffsetLookup();
            FunctionLookup<Integer, Expression<BigDecimal>, Expression<BigDecimal>> priorCount = new FunctionLookup<>(
                    Integer.class, offsetMax);
            priorCount.setFunction(maxOffset -> Expressions.asNumber(maxOffset).subtract(prior).add(1));
            priorCount.setAlias(PERIOD_COUNT);
            Query priorCountQuery = Query.builder().from(BusinessEntity.Transaction).select(priorCount).build();
            SubQuery priorCountSubQuery = new SubQuery(priorCountQuery, generateAlias(BusinessEntity.Transaction));
            lookup = new SubQueryAttrLookup(priorCountSubQuery, PERIOD_COUNT);
            break;
        case BETWEEN:
            Integer betweenMin = Integer.valueOf(txnRestriction.getTimeFilter().getValues().get(0).toString());
            Integer betweenMax = Integer.valueOf(txnRestriction.getTimeFilter().getValues().get(1).toString());
            lookup = new ValueLookup(betweenMax - betweenMin + 1);
            break;
        default:
            throw new UnsupportedOperationException(
                    "comparator " + txnRestriction.getTimeFilter().getRelation() + " is not supported yet");
        }

        return lookup;
    }

    private String generateAlias(BusinessEntity entity) {
        return entity.name() + RandomStringUtils.randomAlphanumeric(8);
    }

    private Restriction filterByProduct() {
        return Restriction.builder().let(BusinessEntity.Transaction, InterfaceName.ProductId.name())
                .eq(txnRestriction.getProductId()).build();
    }

    private Restriction filterByTime(Lookup periodOffset) {
        Restriction restriction = null;
        switch (txnRestriction.getTimeFilter().getRelation()) {
        case EVER:
            restriction = Restriction.builder().let(periodOffset).isNotNull().build();
            break;
        case EQUAL:
            restriction = Restriction.builder().let(periodOffset).eq(txnRestriction.getTimeFilter().getValues().get(0))
                    .build();
            break;
        case IN_CURRENT_PERIOD:
            restriction = Restriction.builder().let(periodOffset).eq(0).build();
            break;
        case WITHIN:
            restriction = Restriction.builder().let(periodOffset).lte(txnRestriction.getTimeFilter().getValues().get(0))
                    .build();
            break;
        case PRIOR:
            restriction = Restriction.builder().let(periodOffset).gte(txnRestriction.getTimeFilter().getValues().get(0))
                    .build();
            break;
        case BETWEEN:
            Object minOffset = txnRestriction.getTimeFilter().getValues().get(0);
            Object maxOffset = txnRestriction.getTimeFilter().getValues().get(1);
            Restriction minRestriction = Restriction.builder().let(periodOffset).gte(minOffset).build();
            Restriction maxRestriction = Restriction.builder().let(periodOffset).lte(maxOffset).build();
            restriction = Restriction.builder().and(minRestriction, maxRestriction).build();
            break;
        default:
            throw new UnsupportedOperationException(
                    "comparator " + txnRestriction.getTimeFilter().getRelation() + " is not supported yet");
        }
        return restriction;
    }
}
