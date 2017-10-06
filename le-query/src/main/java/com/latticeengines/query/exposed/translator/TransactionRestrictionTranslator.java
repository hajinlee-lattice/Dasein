package com.latticeengines.query.exposed.translator;

import static com.latticeengines.domain.exposed.query.AggregationType.AT_LEAST_ONCE;
import static com.latticeengines.domain.exposed.query.AggregationType.EACH;

import java.math.BigDecimal;
import java.util.Collections;

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
import com.latticeengines.domain.exposed.query.util.ExpressionTemplateUtils;
import com.latticeengines.query.util.RestrictionUtils;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.Expressions;

public class TransactionRestrictionTranslator {

    private TransactionRestriction txnRestriction;

    public final static String PERIOD_AMOUNT = "PeriodAmount";
    public final static String PERIOD_QUANTITY = "PeriodQuantity";
    public final static String PERIOD_MAX = "PeriodMax";

    public TransactionRestrictionTranslator(TransactionRestriction txnRestriction) {
        this.txnRestriction = txnRestriction;
    }

    private FunctionLookup<Integer, Object, String> createPeriodLookup() {
        AttributeLookup transactionDate = new AttributeLookup(BusinessEntity.Transaction,
                InterfaceName.TransactionDate.name());
        AttributeLookup periodId = AttributeLookup.fromString(InterfaceName.PeriodId.name());

        Period p = txnRestriction.getTimeFilter().getPeriod();
        String source = ExpressionTemplateUtils.strAttrToDate(transactionDate.toString());
        String target = ExpressionTemplateUtils.getCurrentDate();
        FunctionLookup<Integer, Object, String> period = new FunctionLookup<Integer, Object, String>(Integer.class,
                periodId).as(InterfaceName.PeriodId.name());
        period.setFunction(args -> ExpressionTemplateUtils.getDateDiffTemplate(p, source, target));
        return period;
    }

    public Restriction convert(BusinessEntity entity) {
        Restriction productRestriction = filterByProduct();
        if (txnRestriction.getTimeFilter() == null) {
            txnRestriction.setTimeFilter(new TimeFilter(ComparisonType.EVER, Period.Day, Collections.emptyList()));
        }
        AttributeLookup amountLookup = new AttributeLookup(BusinessEntity.Transaction,
                InterfaceName.TotalAmount.name());
        AttributeLookup quantityLookup = new AttributeLookup(BusinessEntity.Transaction,
                InterfaceName.TotalQuantity.name());
        AttributeLookup accountId = new AttributeLookup(BusinessEntity.Transaction, InterfaceName.AccountId.name());
        FunctionLookup<Integer, Object, String> period = createPeriodLookup();

        Query innerQuery = Query.builder().from(BusinessEntity.Transaction)
                .select(accountId, period, amountLookup, quantityLookup).where(productRestriction).build();
        SubQuery innerSubQuery = new SubQuery(innerQuery, generateAlias(BusinessEntity.Transaction));

        SubQueryAttrLookup subQueryAccountId = new SubQueryAttrLookup(innerSubQuery, InterfaceName.AccountId.name());
        SubQueryAttrLookup subQueryPeriodId = new SubQueryAttrLookup(innerSubQuery, InterfaceName.PeriodId.name());
        SubQueryAttrLookup subQueryPeriodAmount = new SubQueryAttrLookup(innerSubQuery,
                InterfaceName.TotalAmount.name());
        SubQueryAttrLookup subQueryPeriodQuantity = new SubQueryAttrLookup(innerSubQuery,
                InterfaceName.TotalQuantity.name());

        AggregateLookup periodAmount = AggregateLookup.sum(subQueryPeriodAmount).as(PERIOD_AMOUNT);
        AggregateLookup periodQuantity = AggregateLookup.sum(subQueryPeriodQuantity).as(PERIOD_QUANTITY);

        Restriction timeRestriction = filterByTime(subQueryPeriodId);
        Query middleQuery = Query.builder().from(innerSubQuery)
                .select(subQueryAccountId, subQueryPeriodId, periodAmount, periodQuantity).where(timeRestriction)
                .groupBy(groupByAccountAndPeriod(subQueryAccountId, subQueryPeriodId))
                .having(filterByPeriodAmountQuantity(periodAmount, periodQuantity)).build();

        SubQuery middleSubQuery = new SubQuery(middleQuery, generateAlias(BusinessEntity.Transaction));

        subQueryAccountId = new SubQueryAttrLookup(middleSubQuery, InterfaceName.AccountId.name());
        subQueryPeriodId = new SubQueryAttrLookup(middleSubQuery, InterfaceName.PeriodId.name());
        SubQueryAttrLookup periodTotalAmountLookup = new SubQueryAttrLookup(middleSubQuery, PERIOD_AMOUNT);
        SubQueryAttrLookup periodTotalQuantityLookup = new SubQueryAttrLookup(middleSubQuery, PERIOD_QUANTITY);
        AggregateLookup periodMax = AggregateLookup.max(subQueryPeriodId).as(PERIOD_MAX);

        Query outerQuery = Query.builder().from(middleSubQuery).select(subQueryAccountId, periodMax)
                .groupBy(subQueryAccountId) //
                .having(filterByAggregatedPeriodAmountQuantity(periodTotalAmountLookup, periodTotalQuantityLookup,
                        periodMax))
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
            restriction = RestrictionUtils.convertValueComparison(lookup, filter.getComparisonType(),
                    filter.getValue());
        }
        return restriction;
    }

    private Restriction filterByAggregatedPeriodAmountQuantity(Lookup periodAmountLookup, Lookup periodQuantityLookup,
            Lookup periodMax) {
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
            restriction = getAggregatedRestriction(periodAmountLookup, spentFilter, periodMax);
        } else if (spentFilter == null && unitFilter != null) {
            restriction = getAggregatedRestriction(periodQuantityLookup, unitFilter, periodMax);
        } else {
            Restriction amountRestriction = getAggregatedRestriction(periodAmountLookup, spentFilter, periodMax);
            Restriction quantityRestriction = getAggregatedRestriction(periodQuantityLookup, unitFilter, periodMax);
            restriction = Restriction.builder().and(amountRestriction, quantityRestriction).build();
        }
        return restriction;
    }

    private Restriction getAggregatedRestriction(Lookup mixin, AggregationFilter filter, Lookup periodMax) {
        Restriction restriction = null;
        switch (filter.getAggregationType()) {
        case AVG:
            restriction = RestrictionUtils.convertValueComparison(AggregateLookup.avg(mixin),
                    filter.getComparisonType(), filter.getValue());
            break;
        case SUM:
            restriction = RestrictionUtils.convertValueComparison(AggregateLookup.sum(mixin),
                    filter.getComparisonType(), filter.getValue());
            break;
        case AT_LEAST_ONCE:
            restriction = Restriction.builder().let(AggregateLookup.count()).gt(0).build();
            break;
        case EACH:
            restriction = Restriction.builder().let(AggregateLookup.count()).eq(createPeriodRangeLookup(periodMax))
                    .build();
            break;
        default:
            throw new UnsupportedOperationException("Unsupported aggregation type " + filter.getAggregationType());
        }
        return restriction;
    }

    private FunctionLookup<Integer, Expression<BigDecimal>, Expression<BigDecimal>> createPeriodRangeLookup(
            Lookup periodMax) {
        FunctionLookup<Integer, Expression<BigDecimal>, Expression<BigDecimal>> periodRange = new FunctionLookup<Integer, Expression<BigDecimal>, Expression<BigDecimal>>(
                Integer.class, periodMax).as("PeriodRange");
        periodRange.setFunction(exp -> Expressions.asNumber(exp)
                .add(new BigDecimal(txnRestriction.getTimeFilter().getValues().get(0).toString()).negate()));
        return periodRange;
    }

    private String generateAlias(BusinessEntity entity) {
        return entity.name() + RandomStringUtils.randomAlphanumeric(8);
    }

    private Restriction filterByProduct() {
        return Restriction.builder().let(BusinessEntity.Transaction, InterfaceName.ProductId.name())
                .eq(txnRestriction.getProductId()).build();
    }

    private Restriction filterByTime(Lookup periodId) {
        Restriction restriction = null;
        switch (txnRestriction.getTimeFilter().getRelation()) {
        case EVER:
            restriction = Restriction.builder().let(periodId).isNotNull().build();
            break;
        case IN_CURRENT_PERIOD:
            restriction = Restriction.builder().let(periodId).eq(0).build();
            break;
        case BEFORE:
            restriction = Restriction.builder().let(periodId).gt(txnRestriction.getTimeFilter().getValues().get(0))
                    .build();
            break;
        case AFTER:
            restriction = Restriction.builder().let(periodId).lt(txnRestriction.getTimeFilter().getValues().get(0))
                    .build();
            break;
        default:
            throw new UnsupportedOperationException(
                    "comparator " + txnRestriction.getTimeFilter().getRelation() + " is not supported yet");
        }
        return restriction;
    }
}
