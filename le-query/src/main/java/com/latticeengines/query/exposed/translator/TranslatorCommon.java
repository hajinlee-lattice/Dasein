package com.latticeengines.query.exposed.translator;

import static com.latticeengines.query.exposed.translator.TranslatorUtils.generateAlias;
import static com.latticeengines.query.exposed.translator.TranslatorUtils.toAggregatedBooleanExpression;
import static com.latticeengines.query.exposed.translator.TranslatorUtils.toBooleanExpression;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Stream;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AggregationType;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.querydsl.core.types.EntityPath;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.NumberExpression;
import com.querydsl.core.types.dsl.NumberPath;
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.sql.SQLExpressions;
import com.querydsl.sql.SQLQuery;
import com.querydsl.sql.SQLQueryFactory;
import com.querydsl.sql.WindowFunction;

@SuppressWarnings({ "unchecked", "rawtypes" })
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
    static final String TRXN_REVENUE = "trxnrevenue";
    static final String SHIFTED_REVENUE = "trxnshift";
    static final String AMOUNT_AGG = "amountagg";
    static final String QUANTITY_AGG = "quantityagg";
    static final String AMOUNT_VAL = "amountval";
    static final String QUANTITY_VAL = "quantityval";
    static final String KEYS = "tempkeys";
    static final String TEMP_TRXN = "temptrxn";
    static final String SEGMENT = "segment";
    static final String TRXN_PERIOD = "trxnperiod";
    static final String NUMBERS = "numbers";
    static final String REVENUE = "revenue";
    static final String NUMBER = "n";
    static final String DUMMY = "dummy";
    static final String PERIOD_RANGE = "periodrange";
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

    static final EntityPath<String> revenuePath = new PathBuilder<>(String.class, TRXN_REVENUE);
    static final StringPath revenueAccountId = Expressions.stringPath(revenuePath, ACCOUNT_ID);
    static final StringPath revenuePeriodId = Expressions.stringPath(revenuePath, PERIOD_ID);
    static final StringPath revenueRevenue = Expressions.stringPath(revenuePath, REVENUE);

    static final EntityPath<String> shiftedRevenuePath = new PathBuilder<>(String.class, SHIFTED_REVENUE);
    static final StringPath shiftedAccountId = Expressions.stringPath(shiftedRevenuePath, ACCOUNT_ID);
    static final StringPath shiftedPeriodId = Expressions.stringPath(shiftedRevenuePath, PERIOD_ID);
    static final StringPath shiftedRevenue = Expressions.stringPath(shiftedRevenuePath, REVENUE);

    static BooleanExpression translateAggregatePredicate(StringPath stringPath, AggregationFilter aggregationFilter,
            boolean aggregate) {
        AggregationType aggregateType = aggregationFilter.getAggregationType();
        ComparisonType cmp = aggregationFilter.getComparisonType();
        List<Object> values = aggregationFilter.getValues();

        BooleanExpression aggrPredicate = null;
        switch (aggregateType) {
        case SUM:
        case AVG:
        case AT_LEAST_ONCE:
        case EACH:
            aggrPredicate = (aggregate) ? toAggregatedBooleanExpression(stringPath, cmp, values)
                    : toBooleanExpression(stringPath, cmp, values);
            break;
        }

        return aggrPredicate;
    }

    private static boolean isNotLessThanOperation(ComparisonType cmp) {
        return (ComparisonType.LESS_THAN != cmp && ComparisonType.LESS_OR_EQUAL != cmp);
    }

    private static boolean excludeNotPurchasedInLessThanOperation(AggregationFilter aggregationFilter) {
        return !(aggregationFilter == null || isNotLessThanOperation(aggregationFilter.getComparisonType())
                || aggregationFilter.isIncludeNotPurchased());
    }

    private WindowFunction translateEver(WindowFunction windowAgg) {
        return windowAgg.rows().between().unboundedPreceding().currentRow();
    }

    private WindowFunction translatePrior(WindowFunction windowAgg, int priorOffset) {
        return windowAgg.rows().between().unboundedPreceding().preceding(priorOffset);
    }

    private WindowFunction translateBetween(WindowFunction windowAgg, int startOffset, int endOffset,
            boolean preceding) {
        // for row preceding, SQL requires we start with the larger offset,
        // order matters
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
            return translateBetween(windowAgg, Integer.valueOf(timeFilter.getValues().get(0).toString()),
                    Integer.valueOf(timeFilter.getValues().get(1).toString()), true);
        } else if (ComparisonType.WITHIN == type) {
            return translateBetween(windowAgg, Integer.valueOf(timeFilter.getValues().get(0).toString()), 0, true);
        } else if (ComparisonType.PRIOR == type) {
            return translatePrior(windowAgg, Integer.valueOf(timeFilter.getValues().get(0).toString()));
        } else if (ComparisonType.FOLLOWING == type) {
            return translateBetween(windowAgg, Integer.valueOf(timeFilter.getValues().get(0).toString()),
                    Integer.valueOf(timeFilter.getValues().get(1).toString()), false);
        } else if (ComparisonType.IN_CURRENT_PERIOD == type) {
            return translateBetween(windowAgg, 0, 0, true);
        } else if (ComparisonType.EQUAL == type) {
            return translateBetween(windowAgg, Integer.valueOf(timeFilter.getValues().get(0).toString()),
                    Integer.valueOf(timeFilter.getValues().get(0).toString()), true);
        } else {
            throw new UnsupportedOperationException("Unsupported time filter type " + type);
        }
    }

    WindowFunction translateAggregateTimeWindow(StringPath keysAccountId, StringPath keysPeriodId, StringPath trxnVal,
            TimeFilter timeFilter, AggregationFilter aggregationFilter, boolean ascendingPeriod) {
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

    public static boolean isHasEngagedRestriction(TransactionRestriction txRestriction) {
        return (txRestriction.getSpentFilter() == null && txRestriction.getUnitFilter() == null);
    }

    public static Restriction translateHasEngagedToLogicalGroup(TransactionRestriction hasEngaged) {
        // PLS-8016, fix is commented out to match playmaker behavior
        /*
         * if (hasEngaged.getTimeFilter().getRelation() ==
         * ComparisonType.PRIOR_ONLY && !hasEngaged.isNegate()) { return
         * hasEngaged; }
         */

        // no need to translate if it's for single product
        if (hasEngaged.getProductId().split(",").length == 1) {
            return hasEngaged;
        }

        TransactionRestriction[] restrictionList;
        restrictionList = Stream
                .of(hasEngaged.getProductId().split(",")).map(prodId -> new TransactionRestriction(prodId,
                        hasEngaged.getTimeFilter(), hasEngaged.isNegate(), null, null))
                .toArray(TransactionRestriction[]::new);

        if (hasEngaged.isNegate()) {
            return Restriction.builder().and(restrictionList).build();
        } else {
            return Restriction.builder().or(restrictionList).build();

        }
    }

    public static boolean excludeNotPurchased(TransactionRestriction txRestriction) {
        return excludeNotPurchasedInLessThanOperation(txRestriction.getSpentFilter())
                || excludeNotPurchasedInLessThanOperation(txRestriction.getUnitFilter());
    }

    public static Restriction translateExcludeNotPurchased(TransactionRestriction txRestriction) {

        TransactionRestriction hasPurchased = new TransactionRestriction(txRestriction.getProductId(),
                txRestriction.getTimeFilter(), false, null, null);
        return Restriction.builder().and(txRestriction, hasPurchased).build();
    }

    protected SubQuery translateAPSUnionAll(QueryFactory queryFactory, AttributeRepository repository,
            SubQuery[] apsSubQueryList, String sqlUser) {
        SQLQueryFactory factory = queryFactory.getSQLQueryFactory(repository, sqlUser);
        SQLQuery[] apsSelectAlls = Stream.of(apsSubQueryList).map(apsQuery -> factory.query().select(SQLExpressions.all)
                .from(new PathBuilder<>(String.class, apsQuery.getAlias()))).toArray(SQLQuery[]::new);

        SQLQuery unionAll = factory.query().select(SQLExpressions.all).from(SQLExpressions.unionAll(apsSelectAlls));

        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(unionAll);
        subQuery.setAlias(generateAlias(APS));
        return subQuery.withProjections(ACCOUNT_ID, PERIOD_ID, AMOUNT_AGG, QUANTITY_AGG);
    }

    protected SubQuery translateAPSUnionAllReplaceNull(QueryFactory queryFactory, AttributeRepository repository,
            String apsTableName, String sqlUser) {
        SQLQueryFactory factory = queryFactory.getSQLQueryFactory(repository, sqlUser);

        EntityPath<String> apsUnionAllPath = new PathBuilder<>(String.class, apsTableName);
        NumberPath amountNumberPath = Expressions.numberPath(BigDecimal.class, amountAggr.getMetadata());
        NumberPath quantityNumberPath = Expressions.numberPath(BigDecimal.class, quantityAggr.getMetadata());

        NumberExpression zero = Expressions.asNumber(0);
        CaseBuilder caseBuilder = new CaseBuilder();
        CaseBuilder.Cases<BigDecimal, NumberExpression<BigDecimal>> amountAggrCases = caseBuilder.when(amountAggr.isNull())
                .then(zero);
        NumberExpression amountExpr = amountAggrCases.otherwise(amountNumberPath).as(AMOUNT_AGG);
        CaseBuilder.Cases<BigDecimal, NumberExpression<BigDecimal>> quantityAggrCases = caseBuilder.when(quantityAggr.isNull())
                .then(zero);
        NumberExpression quantityExpr = quantityAggrCases.otherwise(quantityNumberPath).as(QUANTITY_AGG);

        SQLQuery apsUnionAllNotNull = factory.query().select(accountId, periodId, amountExpr, quantityExpr)
                .from(apsUnionAllPath);

        SubQuery subQuery = new SubQuery();
        subQuery.setAlias(generateAlias(APS));
        subQuery.setSubQueryExpression(apsUnionAllNotNull);
        return subQuery.withProjections(ACCOUNT_ID, PERIOD_ID, AMOUNT_AGG, QUANTITY_AGG);
    }
}
