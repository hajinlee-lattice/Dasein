package com.latticeengines.query.exposed.translator;

import java.math.BigDecimal;
import java.util.List;

import com.querydsl.core.types.EntityPath;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.NumberExpression;
import com.querydsl.core.types.dsl.NumberPath;
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.sql.SQLExpressions;
import com.querydsl.sql.WindowFunction;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AggregationType;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;

import static com.latticeengines.query.exposed.translator.TranslatorUtils.toBooleanExpression;

public class TranslatorCommon {
    static final int NUM_ADDITIONAL_PERIOD = 2;
    static final String ACCOUNT_ID = InterfaceName.AccountId.name();
    static final String PERIOD_ID = InterfaceName.PeriodId.name();
    static final String PERIOD_NAME = InterfaceName.PeriodName.name();
    static final String PRODUCT_ID = InterfaceName.ProductId.name();
    static final String TOTAL_AMOUNT = InterfaceName.TotalAmount.name();
    static final String TOTAL_QUANTITY = InterfaceName.TotalQuantity.name();
    static final String TRXN = "trxn";
    static final String APS = "aps";
    static final String AMOUNT_AGG = "amountagg";
    static final String QUANTITY_AGG = "quantityagg";
    static final String AMOUNT_VAL = "amountval";
    static final String QUANTITY_VAL = "quantityval";
    static final String KEYS = "keys";
    static final String TRXN_PERIOD = "trxnbyperiod";
    static final String NUMBERS = "numbers";
    static final String NUMBER = "n";
    static final String DUMMY = "dummy";
    static final String PERIOD_RANGE = "periodRange";
    static final String MIN_PID = "minpid";
    static final String CROSS_PROD = "crossprod";
    static final String ALL_ACCOUNTS = "allaccounts";
    static final String ALL_PERIODS = "allperiods";
    static final String MAX_PID = "maxpid";

    static final StringPath accountId = Expressions.stringPath(ACCOUNT_ID);
    static final StringPath periodId = Expressions.stringPath(PERIOD_ID);
    static final StringPath periodName = Expressions.stringPath(PERIOD_NAME);
    static final StringPath productId = Expressions.stringPath(PRODUCT_ID);
    static final StringPath amountVal = Expressions.stringPath(TOTAL_AMOUNT);
    static final StringPath quantityVal = Expressions.stringPath(TOTAL_QUANTITY);
    static final StringPath amountAggr = Expressions.stringPath(AMOUNT_AGG);
    static final StringPath quantityAggr = Expressions.stringPath(QUANTITY_AGG);
    static final StringPath allPeriods = Expressions.stringPath(ALL_PERIODS);

    static final EntityPath<String> apsPath = new PathBuilder<>(String.class, APS);
    static final EntityPath<String> periodRange = new PathBuilder<>(String.class, PERIOD_RANGE);
    static final StringPath periodAccountId = Expressions.stringPath(periodRange, ACCOUNT_ID);
    static final EntityPath<String> crossProd = new PathBuilder<>(String.class, CROSS_PROD);
    static final StringPath crossAccountId = Expressions.stringPath(crossProd, ACCOUNT_ID);
    static final StringPath crossPeriodId = Expressions.stringPath(crossProd, PERIOD_ID);

    static final EntityPath<String> trxnPath = new PathBuilder<>(String.class, TRXN);
    static final StringPath trxnAccountId = Expressions.stringPath(trxnPath, ACCOUNT_ID);
    static final StringPath trxnPeriodId = Expressions.stringPath(trxnPath, PERIOD_ID);
    static final StringPath trxnAmountVal = Expressions.stringPath(trxnPath, AMOUNT_VAL);
    static final StringPath trxnQuantityVal = Expressions.stringPath(trxnPath, QUANTITY_VAL);

    static final EntityPath<String> keysPath = new PathBuilder<>(String.class, KEYS);
    static final StringPath keysAccountId = Expressions.stringPath(keysPath, ACCOUNT_ID);
    static final StringPath keysPeriodId = Expressions.stringPath(keysPath, PERIOD_ID);

    BooleanExpression translateAggregatePredicate(StringPath aggr, AggregationFilter aggregationFilter) {
        AggregationType aggregateType = aggregationFilter.getAggregationType();
        ComparisonType cmp = aggregationFilter.getComparisonType();
        List<Object> values = aggregationFilter.getValues();

        BooleanExpression aggrPredicate = null;
        switch (aggregateType) {
        case SUM:
        case AVG:
        case AT_LEAST_ONCE:
        case EACH:
            aggrPredicate = toBooleanExpression(aggr, cmp, values);
            break;
        }

        return aggrPredicate;
    }

    private boolean isNotLessThanOperation(ComparisonType cmp) {
        return (ComparisonType.LESS_THAN != cmp && ComparisonType.LESS_OR_EQUAL != cmp);
    }

    @SuppressWarnings("unchecked")
    private WindowFunction translateEver(WindowFunction windowAgg) {
        return windowAgg.rows().between().unboundedPreceding().currentRow();
    }

    @SuppressWarnings("unchecked")
    private WindowFunction translatePrior(WindowFunction windowAgg,
                                          int priorOffset) {
        return windowAgg.rows().between().unboundedPreceding().preceding(priorOffset);
    }

    @SuppressWarnings("unchecked")
    private WindowFunction translateBetween(WindowFunction windowAgg,
                                            int startOffset, int endOffset,
                                            boolean preceding) {
        // for row preceding, SQL requires we start with the larger offset, order matters
        int minOffset = Math.min(startOffset, endOffset);
        int maxOffset = Math.max(startOffset, endOffset);
        if (preceding) {
            return windowAgg.rows().between().preceding(maxOffset).preceding(minOffset);
        } else {
            return windowAgg.rows().between().following(minOffset).following(maxOffset);
        }
    }

    WindowFunction translateTimeWindow(TimeFilter timeFilter, WindowFunction windowAgg) {
        ComparisonType type = timeFilter.getRelation();
        if (ComparisonType.EVER == type) {
            return translateEver(windowAgg);
        } else if (ComparisonType.BETWEEN == type) {
            return translateBetween(windowAgg,
                                    Integer.valueOf(timeFilter.getValues().get(0).toString()),
                                    Integer.valueOf(timeFilter.getValues().get(1).toString()),
                                    true);
        } else if (ComparisonType.WITHIN == type) {
            return translateBetween(windowAgg,
                                    Integer.valueOf(timeFilter.getValues().get(0).toString()),
                                    0,
                                    true);
        } else if (ComparisonType.PRIOR == type) {
            return translatePrior(windowAgg,
                                  Integer.valueOf(timeFilter.getValues().get(0).toString()));
        } else if (ComparisonType.FOLLOWING == type) {
            return translateBetween(windowAgg,
                                    Integer.valueOf(timeFilter.getValues().get(0).toString()),
                                    Integer.valueOf(timeFilter.getValues().get(0).toString()),
                                    false);
        } else if (ComparisonType.IN_CURRENT_PERIOD == type) {
            return translateBetween(windowAgg, 0, 0, true);
        } else if (ComparisonType.EQUAL == type) {
            return translateBetween(windowAgg,
                                    Integer.valueOf(timeFilter.getValues().get(0).toString()),
                                    Integer.valueOf(timeFilter.getValues().get(0).toString()),
                                    true);
        } else {
            throw new UnsupportedOperationException("Unsupported time filter type " + type);
        }
    }

    @SuppressWarnings("unchecked")
    WindowFunction translateAggregateTimeWindow(StringPath keysAccountId,
                                                StringPath keysPeriodId,
                                                StringPath trxnVal,
                                                TimeFilter timeFilter,
                                                AggregationFilter aggregationFilter,
                                                boolean ascendingPeriod) {
        NumberExpression trxnValNumber = Expressions.numberPath(BigDecimal.class, trxnVal.getMetadata());


        AggregationType aggregateType = aggregationFilter.getAggregationType();
        ComparisonType cmp = aggregationFilter.getComparisonType();
        boolean isNotLessComparison = isNotLessThanOperation(cmp);

        WindowFunction windowAgg = null;
        switch (aggregateType) {
        case SUM:
            windowAgg = SQLExpressions.sum(trxnValNumber.coalesce(BigDecimal.ZERO)).over();
            break;
        case AVG:
            windowAgg = SQLExpressions.avg(trxnValNumber.coalesce(BigDecimal.ZERO)).over();
            break;
        case AT_LEAST_ONCE:
            if (isNotLessComparison) {
                windowAgg = SQLExpressions.max(trxnValNumber.coalesce(BigDecimal.ZERO)).over();
            } else {
                windowAgg = SQLExpressions.min(trxnValNumber.coalesce(BigDecimal.ZERO)).over();
            }
            break;
        case EACH:
            if (isNotLessComparison) {
                windowAgg = SQLExpressions.min(trxnValNumber.coalesce(BigDecimal.ZERO)).over();
            } else {
                windowAgg = SQLExpressions.max(trxnValNumber.coalesce(BigDecimal.ZERO)).over();
            }
            break;
        }

        if (ascendingPeriod) {
            windowAgg.partitionBy(keysAccountId).orderBy(keysPeriodId);
        } else {
            windowAgg.partitionBy(keysAccountId).orderBy(keysPeriodId.desc());
        }

        return translateTimeWindow(timeFilter, windowAgg);
    }
}
