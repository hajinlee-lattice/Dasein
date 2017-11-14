package com.latticeengines.query.exposed.translator;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AggregationType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.QueryBuilder;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.querydsl.core.types.EntityPath;
import com.querydsl.core.types.Expression;
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

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AggregatedTransaction;

public class NewTransactionRestrictionTranslator {
    public static final int NUM_ADDITIONAL_PERIOD = 2;
    public static final int ONE_LEG_BEHIND_OFFSET = 1;
    public static final String ACCOUNT_ID = InterfaceName.AccountId.name();
    public static final String PERIOD_ID = InterfaceName.PeriodId.name();
    public static final String PRODUCT_ID = InterfaceName.ProductId.name();
    public static final String TOTAL_AMOUNT = InterfaceName.TotalAmount.name();
    public static final String TOTAL_QUANTITY = InterfaceName.TotalQuantity.name();
    public static final String TRXN = "trxn";
    public static final String APS = "aps";
    public static final String AMOUNT_AGG = "amountagg";
    public static final String QUANTITY_AGG = "quantityagg";
    public static final String AMOUNT_VAL = "amountval";
    public static final String QUANTITY_VAL = "quantityval";
    public static final String KEYS = "keys";
    public static final String TRXN_PERIOD = "trxnbyperiod";
    public static final String NUMBERS = "numbers";
    public static final String NUMBER = "n";
    public static final String DUMMY = "dummy";
    public static final String ALL_ACCOUNTS = "allaccounts";
    public static final String ALL_PERIODS = "allperiods";
    public static final String MAX_PID = "maxpid";

    private StringPath accountId = Expressions.stringPath(ACCOUNT_ID);
    private StringPath periodId = Expressions.stringPath(PERIOD_ID);
    private StringPath productId = Expressions.stringPath(PRODUCT_ID);
    private StringPath amountVal = Expressions.stringPath(TOTAL_AMOUNT);
    private StringPath quantityVal = Expressions.stringPath(TOTAL_QUANTITY);
    private StringPath amountAggr = Expressions.stringPath(AMOUNT_AGG);
    private StringPath quantityAggr = Expressions.stringPath(QUANTITY_AGG);

    private EntityPath<String> periodRange = new PathBuilder<>(String.class, "periodRange");
    private NumberPath periodRangeMaxPid = Expressions.numberPath(BigDecimal.class, periodRange, "maxpid");
    private StringPath periodAccountId = Expressions.stringPath(periodRange, ACCOUNT_ID);
    private EntityPath<String> crossProd = new PathBuilder<>(String.class, "crossprod");
    private StringPath crossAccountId = Expressions.stringPath(crossProd, ACCOUNT_ID);
    private StringPath crossPeriodId = Expressions.stringPath(crossProd, PERIOD_ID);
    private StringPath numberTable = Expressions.stringPath("numbers");
    private StringPath allPeriods = Expressions.stringPath(ALL_PERIODS);

    private EntityPath<String> keysPath = new PathBuilder<>(String.class, KEYS);
    private EntityPath<String> trxnPath = new PathBuilder<>(String.class, TRXN);
    private EntityPath<String> apsPath = new PathBuilder<>(String.class, APS);
    private StringPath keysAccountId = Expressions.stringPath(keysPath, ACCOUNT_ID);
    private StringPath trxnAccountId = Expressions.stringPath(trxnPath, ACCOUNT_ID);
    private StringPath keysPeriodId = Expressions.stringPath(keysPath, PERIOD_ID);
    private StringPath trxnPeriodId = Expressions.stringPath(trxnPath, PERIOD_ID);
    private StringPath trxnAmountVal = Expressions.stringPath(trxnPath, AMOUNT_VAL);
    private StringPath trxnQuantityVal = Expressions.stringPath(trxnPath, QUANTITY_VAL);
    private StringPath trxnVal = Expressions.stringPath(trxnPath, AMOUNT_VAL);

    private SQLQueryFactory getSQLQueryFactory(QueryFactory queryFactory, AttributeRepository repository) {
        return queryFactory.getSQLQueryFactory(repository);
    }

    protected String getTransactionTableName(AttributeRepository repository) {
        return repository.getTableName(AggregatedTransaction);
    }

    protected String getPeriodTransactionTableName() {
        return TRXN_PERIOD;
    }

    public Restriction convert(TransactionRestriction txnRestriction,
                               QueryFactory queryFactory,
                               AttributeRepository repository,
                               BusinessEntity entity,
                               QueryBuilder queryBuilder) {
        if (txnRestriction.getTimeFilter() == null) {
            txnRestriction.setTimeFilter(TimeFilter.ever());
        }

        // treat PRIOR_ONLY specially to match playmaker functionality
        if (ComparisonType.PRIOR_ONLY == txnRestriction.getTimeFilter().getRelation()) {

            if (txnRestriction.isNegate()) {
                Restriction notPriorRestriction = translateToPrior(
                        queryFactory, repository, entity, txnRestriction, true, queryBuilder);
                Restriction withinRestriction = translateToHasNotPurchasedWithin(
                        queryFactory, repository, entity, txnRestriction, true, queryBuilder);
                return Restriction.builder().or(notPriorRestriction, withinRestriction).build();
            } else {
                Restriction priorRestriction = translateToPrior(
                        queryFactory, repository, entity, txnRestriction, false, queryBuilder);
                Restriction notWithinRestriction = translateToHasNotPurchasedWithin(
                        queryFactory, repository, entity, txnRestriction, false, queryBuilder);
                return Restriction.builder().and(priorRestriction, notWithinRestriction).build();
            }
        } else {
            return translateRestriction(queryFactory, repository, entity, txnRestriction, queryBuilder);
        }
    }


    @SuppressWarnings({"unchecked", "rawtype"})
    private SubQuery translateAllKeys(QueryFactory queryFactory, AttributeRepository repository) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository);

        String periodTableName = getPeriodTransactionTableName();

        StringPath tablePath = Expressions.stringPath(periodTableName);
        NumberExpression maxPid = Expressions.numberTemplate(BigDecimal.class, PERIOD_ID).max().as(MAX_PID);

        SQLQuery periodRangeSubQuery =
                factory.query().select(accountId, maxPid).from(tablePath).groupBy(accountId);

        SQLQuery crossProdQuery = factory.query().select(accountId, periodId).from(
                factory.selectDistinct(accountId).from(tablePath).as(ALL_ACCOUNTS),
                factory.select(periodId).from(allPeriods));

        SQLQuery crossProdSubQuery = factory.query().from(crossProdQuery, crossProd)
                .innerJoin(periodRangeSubQuery, periodRange).on(periodAccountId.eq(crossAccountId))
                .where(crossPeriodId.loe(periodRangeMaxPid.add(NUM_ADDITIONAL_PERIOD)));

        SQLQuery minusProdSubQuery = crossProdSubQuery.select(crossAccountId, periodId);

        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(minusProdSubQuery);
        subQuery.setAlias(KEYS);
        return subQuery.withProjection(ACCOUNT_ID).withProjection(PERIOD_ID);
    }

    private static String getDateDiffTemplate(TimeFilter.Period p) {
        return String.format("DATEDIFF('%s', date(%s), current_date)", p.name(), "transactiondate");
    }

    @SuppressWarnings("unchecked")
    public SubQuery translateNumberSeries(QueryFactory queryFactory,
                                          AttributeRepository repository) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository);

        String txTableName = getTransactionTableName(repository);
        StringPath tablePath = Expressions.stringPath(txTableName);

        // has to use a dummy column or querydsl will not generate the right sql
        SQLQuery numberQuery = factory.query()
                .select(SQLExpressions.rowNumber().over(), Expressions.constant(1))
                .from(tablePath);

        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(numberQuery);
        subQuery.setAlias(NUMBERS);
        return subQuery.withProjection(NUMBER).withProjection(DUMMY);
    }

    @SuppressWarnings("unchecked")
    public SubQuery translateAllPeriods(QueryFactory queryFactory,
                                        AttributeRepository repository,
                                        TimeFilter.Period period) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository);

        String txTableName = getTransactionTableName(repository);
        StringPath tablePath = Expressions.stringPath(txTableName);
        NumberPath number = Expressions.numberPath(BigDecimal.class, NUMBER);
        Expression<?> maxPeriodOffset =
                Expressions.numberTemplate(BigDecimal.class, getDateDiffTemplate(period)).max().add(1);
        SQLQuery maxPeriodQuery = factory.query().from(tablePath).select(maxPeriodOffset);

        // has to use a dummy column or querydsl will not generate the right sql
        SQLQuery allPeriodsQuery = factory.query()
                .select((Expression<?>) number.subtract(1), Expressions.constant(1))
                .from(numberTable)
                .where(number.loe(maxPeriodQuery));


        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(allPeriodsQuery);
        subQuery.setAlias(ALL_PERIODS);
        return subQuery.withProjection(PERIOD_ID).withProjection(DUMMY);
    }

    @SuppressWarnings("unchecked")
    public SubQuery translatePeriodTransaction(QueryFactory queryFactory,
                                               AttributeRepository repository,
                                               TimeFilter.Period period) {

        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository);

        String txTableName = getTransactionTableName(repository);
        StringPath tablePath = Expressions.stringPath(txTableName);
        Expression<?> totalAmountSum = Expressions.numberPath(BigDecimal.class, TOTAL_AMOUNT).sum().coalesce(BigDecimal.ZERO);
        Expression<?> totalQuantitySum = Expressions.numberPath(BigDecimal.class, TOTAL_QUANTITY).sum().coalesce(BigDecimal.ZERO);
        Expression<?> periodOffset =
                Expressions.numberTemplate(BigDecimal.class, getDateDiffTemplate(period));


        SQLQuery trxnByPeriodSubQuery = factory.from(tablePath)
                .select(totalAmountSum, totalQuantitySum, periodOffset, productId, accountId)
                .groupBy(periodOffset, productId, accountId);


        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(trxnByPeriodSubQuery);
        subQuery.setAlias(TRXN_PERIOD);
        return subQuery.withProjection(TOTAL_AMOUNT)
                .withProjection(TOTAL_QUANTITY)
                .withProjection(PERIOD_ID)
                .withProjection(PRODUCT_ID)
                .withProjection(ACCOUNT_ID);

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

    private BooleanExpression translateAggregatePredicate(StringPath aggr, AggregationFilter aggregationFilter) {
        AggregationType aggregateType = aggregationFilter.getAggregationType();
        ComparisonType cmp = aggregationFilter.getComparisonType();
        List<Object> values = aggregationFilter.getValues();

        BooleanExpression aggrPredicate = null;
        switch (aggregateType) {
        case SUM:
        case AVG:
            aggrPredicate = toBooleanExpression(aggr, cmp, values);
            break;
        case AT_LEAST_ONCE:
        case EACH:
            aggrPredicate = aggr.eq(String.valueOf(1));
            break;
        }

        return aggrPredicate;
    }

    private BooleanExpression toBooleanExpression(StringPath numberPath, ComparisonType cmp, List<Object> values) {
        switch (cmp) {
        case GTE_AND_LTE:
            return numberPath.goe(values.get(0).toString()).and(numberPath.loe(values.get(1).toString()));
        case GT_AND_LTE:
            return numberPath.gt(values.get(0).toString()).and(numberPath.loe(values.get(1).toString()));
        case GT_AND_LT:
            return numberPath.gt(values.get(0).toString()).and(numberPath.lt(values.get(1).toString()));
        case GTE_AND_LT:
            return numberPath.goe(values.get(0).toString()).and(numberPath.lt(values.get(1).toString()));
        case GREATER_OR_EQUAL:
            return numberPath.goe(values.get(0).toString());
        case GREATER_THAN:
            return numberPath.gt(values.get(0).toString());
        case LESS_THAN:
            return numberPath.lt(values.get(0).toString());
        case LESS_OR_EQUAL:
            return numberPath.loe(values.get(0).toString());
        case EQUAL:
            return numberPath.eq(values.get(0).toString());
        default:
            throw new UnsupportedOperationException("Unsupported comparison type " + cmp);
        }
    }

    @SuppressWarnings("unchecked")
    private WindowFunction translateAggregateTimeWindow(StringPath keysAccountId,
                                                        StringPath keysPeriodId,
                                                        StringPath trxnVal,
                                                        TimeFilter timeFilter,
                                                        AggregationFilter aggregationFilter,
                                                        boolean ascendingPeriod) {
        NumberExpression trxnValNumber = Expressions.numberPath(BigDecimal.class, trxnVal.getMetadata());


        AggregationType aggregateType = aggregationFilter.getAggregationType();
        ComparisonType cmp = aggregationFilter.getComparisonType();
        List<Object> values = aggregationFilter.getValues();

        WindowFunction windowAgg = null;
        switch (aggregateType) {
        case SUM:
            windowAgg = SQLExpressions.sum(trxnValNumber).over();
            break;
        case AVG:
            windowAgg = SQLExpressions.avg(trxnValNumber).over();
            break;
        case AT_LEAST_ONCE:
            BooleanExpression condition = toBooleanExpression(trxnVal, cmp, values);
            NumberExpression trxnValExists = new CaseBuilder().when(condition).then(1).otherwise(0);
            windowAgg = SQLExpressions.max(trxnValExists).over();
            break;
        case EACH:
            BooleanExpression each = toBooleanExpression(trxnVal, cmp, values);
            NumberExpression trxnValExistsOrNull = new CaseBuilder().when(each).then(1).otherwise(0);
            windowAgg = SQLExpressions.min(trxnValExistsOrNull).over();
            break;
        }

        if (ascendingPeriod) {
            windowAgg.partitionBy(keysAccountId).orderBy(keysPeriodId);
        } else {
            windowAgg.partitionBy(keysAccountId).orderBy(keysPeriodId.desc());
        }

        return translateTimeWindow(timeFilter, windowAgg);
    }

    private WindowFunction translateTimeWindow(TimeFilter timeFilter, WindowFunction windowAgg) {
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

    private BooleanExpression translateProductId(String productIdStr) {
        return productId.in(productIdStr.split(","));
    }

    @SuppressWarnings("unchecked")
    private BooleanExpression translatePeriodRestriction(StringPath periodId) {
        return periodId.eq(Expressions.constant(0));
    }

    @SuppressWarnings("unchecked")
    private SQLQuery translateTransaction(QueryFactory queryFactory,
                                          AttributeRepository repository,
                                          TransactionRestriction txRestriction) {

        TimeFilter timeFilter = txRestriction.getTimeFilter();

        if (txRestriction.getSpentFilter() == null && txRestriction.getUnitFilter() == null) {
            return translateHasEngaged(queryFactory, repository, txRestriction);
        }

        AggregationFilter spentFilter = txRestriction.getSpentFilter();
        AggregationFilter unitFilter = txRestriction.getUnitFilter();

        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository);

        String txTableName = getPeriodTransactionTableName();
        StringPath tablePath = Expressions.stringPath(txTableName);

        List<Expression> productSelectList = new ArrayList<>();
        productSelectList.addAll(Arrays.asList(accountId, periodId));
        List<Expression> apsSelectList = new ArrayList<>();
        apsSelectList.addAll(Arrays.asList(keysAccountId, keysPeriodId));

        if (spentFilter != null) {
            productSelectList.add(amountVal.as(AMOUNT_VAL));
            Expression spentWindowAgg = translateAggregateTimeWindow(
                    keysAccountId, keysPeriodId, trxnAmountVal, timeFilter, spentFilter, false).as(amountAggr);
            apsSelectList.add(trxnAmountVal);
            apsSelectList.add(spentWindowAgg);
        }

        if (unitFilter != null) {
            productSelectList.add(quantityVal.as(QUANTITY_VAL));
            Expression unitWindowAgg = translateAggregateTimeWindow(
                    keysAccountId, keysPeriodId, trxnQuantityVal, timeFilter, unitFilter, false).as(quantityAggr);
            apsSelectList.add(trxnQuantityVal);
            apsSelectList.add(unitWindowAgg);
        }


        SQLQuery productQuery = factory.query()
                .select(productSelectList.toArray(new Expression[0]))
                .from(tablePath)
                .where(translateProductId(txRestriction.getProductId()));

        SQLQuery apsQuery = factory.query()
                .select(apsSelectList.toArray(new Expression[0]))
                .from(keysPath)
                .leftJoin(productQuery, trxnPath)
                .on(keysAccountId.eq(trxnAccountId).and(keysPeriodId.eq(trxnPeriodId)));

        BooleanExpression aggrAmountPredicate =
                (spentFilter != null) ? translateAggregatePredicate(amountAggr, spentFilter) : Expressions.TRUE;
        BooleanExpression aggrQuantityPredicate =
                (unitFilter != null) ? translateAggregatePredicate(quantityAggr, unitFilter) : Expressions.TRUE;

        BooleanExpression aggrValPredicate;
        if (!txRestriction.isNegate()) {
            aggrValPredicate = aggrAmountPredicate.and(aggrQuantityPredicate);
        } else {
            aggrValPredicate = aggrAmountPredicate.and(aggrQuantityPredicate).not();
        }
        BooleanExpression periodIdPredicate = translatePeriodRestriction(periodId);

        SQLQuery finalQuery = factory.query().select(accountId, periodId).from(apsQuery, apsPath)
                .where(aggrValPredicate.and(periodIdPredicate));

        return finalQuery;

    }

    @SuppressWarnings("unchecked")
    private SQLQuery translateHasEngaged(QueryFactory queryFactory,
                                         AttributeRepository repository,
                                         TransactionRestriction txRestriction) {

        TimeFilter timeFilter = txRestriction.getTimeFilter();
        String productIdStr = txRestriction.getProductId();

        SQLQueryFactory factory = queryFactory.getSQLQueryFactory(repository);
        String txTableName = getPeriodTransactionTableName();
        StringPath tablePath = Expressions.stringPath(txTableName);

        NumberExpression trxnValNumber = Expressions.numberPath(BigDecimal.class, trxnVal.getMetadata());
        CaseBuilder caseBuilder = new CaseBuilder();
        NumberExpression trxnValExists = caseBuilder.when(trxnValNumber.goe(0)).then(1).otherwise(0);

        Expression windowAgg = translateTimeWindow(timeFilter, SQLExpressions.max(trxnValExists).over()
                .partitionBy(keysAccountId).orderBy(keysPeriodId.desc())).as(amountAggr);

        SQLQuery productQuery = factory.query().select(accountId, periodId, amountVal.as(AMOUNT_VAL)).from(tablePath)
                .where(translateProductId(productIdStr));

        SQLQuery apsQuery = factory.query().select(keysAccountId, keysPeriodId, trxnVal, windowAgg)
                .from(keysPath).leftJoin(productQuery, trxnPath)
                .on(keysAccountId.eq(trxnAccountId).and(keysPeriodId.eq(trxnPeriodId)));

        BooleanExpression periodIdPredicate = translatePeriodRestriction(periodId);

        int expectedResult = 1;

        SQLQuery finalQuery = factory.query().select(accountId, periodId).from(apsQuery, apsPath)
                .where(amountAggr.eq(String.valueOf(expectedResult)).and(periodIdPredicate));

        return finalQuery;

    }

    private SubQuery translateSelectAll(QueryFactory queryFactory,
                                        AttributeRepository repository,
                                        String tableName) {
        SQLQueryFactory factory = queryFactory.getSQLQueryFactory(repository);
        EntityPath<String> tablePath = new PathBuilder<>(String.class, tableName);
        SQLQuery selectAll = factory.query().select(SQLExpressions.all).from(tablePath);
        SubQuery query = new SubQuery();
        query.setSubQueryExpression(selectAll);
        query.setAlias(generateAlias(BusinessEntity.Transaction.name()));
        return query;
    }

    private String generateAlias(String prefix) {
        return prefix + RandomStringUtils.randomAlphanumeric(8);
    }

    private SubQuery translateTransactionRestriction(QueryFactory queryFactory,
                                                     AttributeRepository repository,
                                                     TransactionRestriction txRestriction) {
        SQLQuery subQueryExpression = translateTransaction(queryFactory, repository, txRestriction);
        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(subQueryExpression);
        subQuery.setAlias(generateAlias(BusinessEntity.Transaction.name()));
        return subQuery.withProjection(ACCOUNT_ID).withProjection(PERIOD_ID);
    }

    private boolean hasCommonTableGenerated(QueryBuilder queryBuilder) {
        return queryBuilder.getSubQueryList().size() != 0;
    }

    private Restriction translateToPrior(QueryFactory queryFactory,
                                         AttributeRepository repository,
                                         BusinessEntity businessEntity,
                                         TransactionRestriction res,
                                         boolean negate,
                                         QueryBuilder builder) {

        TimeFilter timeFilter = new TimeFilter(res.getTimeFilter().getLhs(), ComparisonType.PRIOR, //
                                               res.getTimeFilter().getPeriod(), res.getTimeFilter().getValues());
        TransactionRestriction prior = new TransactionRestriction(res.getProductName(), //
                                                                  res.getProductId(), //
                                                                  timeFilter, //
                                                                  negate, //
                                                                  res.getSpentFilter(), //
                                                                  res.getUnitFilter());

        return translateRestriction(queryFactory, repository, businessEntity, prior, builder);
    }

    private Restriction translateToHasNotPurchasedWithin(QueryFactory queryFactory,
                                                         AttributeRepository repository,
                                                         BusinessEntity businessEntity, TransactionRestriction res, //
                                                         boolean negate,
                                                         QueryBuilder builder) {

        TimeFilter timeFilter = new TimeFilter(res.getTimeFilter().getLhs(), ComparisonType.WITHIN, //
                                               res.getTimeFilter().getPeriod(), res.getTimeFilter().getValues());

        TransactionRestriction notWithin = new TransactionRestriction(res.getProductName(), //
                                                                      res.getProductId(), //
                                                                      timeFilter, //
                                                                      !negate, //
                                                                      null, //
                                                                      null);

        return translateRestriction(queryFactory, repository, businessEntity, notWithin, builder);
    }

    public Restriction translateRestriction(QueryFactory queryFactory,
                                            AttributeRepository repository,
                                            BusinessEntity entity,
                                            Restriction restriction,
                                            QueryBuilder builder) {


        // translate restrictions to individual subqueries
        Restriction translated = null;
        if (restriction instanceof TransactionRestriction) {
            TransactionRestriction txRestriction = (TransactionRestriction) restriction;

            if (!hasCommonTableGenerated(builder)) {
                builder.with(translateNumberSeries(queryFactory, repository));
                builder.with(translateAllPeriods(queryFactory, repository, txRestriction.getTimeFilter().getPeriod()));
                builder.with(translatePeriodTransaction(queryFactory, repository,
                                                        txRestriction.getTimeFilter().getPeriod()));
                builder.with(translateAllKeys(queryFactory, repository));
            }

            SubQuery subQuery = translateTransactionRestriction(queryFactory, repository, txRestriction);
            builder.with(subQuery);
            SubQuery selectAll = translateSelectAll(queryFactory, repository, subQuery.getAlias());
            ConcreteRestriction accountInRestriction = (ConcreteRestriction) Restriction.builder()
                    .let(entity, InterfaceName.AccountId.name()).inCollection(selectAll, InterfaceName.AccountId.name())
                    .build();

            translated = txRestriction.isNegate() ? Restriction.builder().not(accountInRestriction).build()
                    : accountInRestriction;
        } else {
            throw new UnsupportedOperationException("Cannot translate restriction " + restriction);
        }


        return translated;
    }

}

