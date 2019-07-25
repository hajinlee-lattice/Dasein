package com.latticeengines.query.exposed.translator;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AggregatedPeriodTransaction;
import static com.latticeengines.query.exposed.translator.TranslatorUtils.generateAlias;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.graph.traversal.impl.BreadthFirstSearch;
import com.latticeengines.common.exposed.graph.traversal.impl.DepthFirstSearch;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.LogicalOperator;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.QueryBuilder;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.util.RestrictionOptimizer;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.latticeengines.query.factory.SparkQueryProvider;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.EntityPath;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.SubQueryExpression;
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
import com.querydsl.sql.Union;
import com.querydsl.sql.WindowFunction;

public class EventQueryTranslator extends TranslatorCommon {
    private static final int ONE_LAG_BEHIND_OFFSET = 1;

    public EventQueryTranslator() {
    }

    public QueryBuilder translateForScoring(QueryFactory queryFactory, AttributeRepository repository,
            Restriction restriction, EventFrontEndQuery frontEndQuery, QueryBuilder queryBuilder, String sqlUser) {
        return translateRestriction(queryFactory, repository, restriction, true, false, frontEndQuery, queryBuilder,
                sqlUser);
    }

    public QueryBuilder translateForTraining(QueryFactory queryFactory, AttributeRepository repository,
            Restriction restriction, EventFrontEndQuery frontEndQuery, QueryBuilder queryBuilder, String sqlUser) {
        return translateRestriction(queryFactory, repository, restriction, false, false, frontEndQuery, queryBuilder,
                sqlUser);
    }

    public QueryBuilder translateForEvent(QueryFactory queryFactory, AttributeRepository repository,
            Restriction restriction, EventFrontEndQuery frontEndQuery, QueryBuilder queryBuilder, String sqlUser) {
        return translateRestriction(queryFactory, repository, restriction, false, true, frontEndQuery, queryBuilder,
                sqlUser);
    }

    private SQLQueryFactory getSQLQueryFactory(QueryFactory queryFactory, AttributeRepository repository,
            String sqlUser) {
        return queryFactory.getSQLQueryFactory(repository, sqlUser);
    }

    protected String getPeriodTransactionTableName(AttributeRepository repository) {
        return repository.getTableName(AggregatedPeriodTransaction);
    }

    private SubQuery translateTempTrxn(QueryFactory queryFactory, AttributeRepository repository, String period, //
            String sqlUser) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);
        String txTableName = getPeriodTransactionTableName(repository);
        StringPath tablePath = Expressions.stringPath(txTableName);

        SQLQuery<Tuple> txnTableSubQuery = factory.query() //
                .select(accountId, periodId, productId, amountVal, quantityVal) //
                .where(periodName.eq(period));
        txnTableSubQuery = txnTableSubQuery.from(tablePath);
        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(txnTableSubQuery);
        subQuery.setAlias(TEMP_TRXN);
        return subQuery;
    }

    @SuppressWarnings("unchecked")
    private SubQuery translateAllKeys(QueryFactory queryFactory, AttributeRepository repository, String sqlUser) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);

        StringPath tablePath = Expressions.stringPath(TEMP_TRXN);
        NumberPath minPid = Expressions.numberPath(BigDecimal.class, periodRange, MIN_PID);
        SQLQuery periodRangeSubQuery = factory.query() //
                .select(accountId, SQLExpressions.min(periodId).as(MIN_PID)) //
                .from(tablePath) //
                .groupBy(accountId);

        SQLQuery crossProdQuery = factory.query().select(accountId, periodId).from(
                factory.selectDistinct(accountId).from(tablePath).as(ALL_ACCOUNTS),
                factory.selectDistinct(periodId).from(tablePath).as(ALL_PERIODS));

        SQLQuery crossProdSubQuery = factory.query().from(crossProdQuery, crossProd)
                .innerJoin(periodRangeSubQuery, periodRange).on(periodAccountId.eq(crossAccountId))
                .where(crossPeriodId.goe(minPid.subtract(NUM_ADDITIONAL_PERIOD)));
        SQLQuery minusProdSubQuery = crossProdSubQuery.select(crossAccountId, periodId);

        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(minusProdSubQuery);
        subQuery.setAlias(KEYS);
        return subQuery.withProjection(ACCOUNT_ID).withProjection(PERIOD_ID);
    }

    private SubQuery translateSegment(Query segmentQuery) {
        SubQuery subQuery = new SubQuery();
        subQuery.setQuery(segmentQuery);
        subQuery.setAlias(SEGMENT);
        return subQuery;
    }

    @SuppressWarnings("unchecked")
    private SubQuery translateProductRevenue(QueryFactory queryFactory, //
            AttributeRepository repository, //
            List<String> targetProductIds, //
            String sqlUser) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);
        StringPath tablePath = Expressions.stringPath(TEMP_TRXN);
        BooleanExpression productFilter = productId.in(targetProductIds);
        NumberExpression trxnAmount = Expressions.numberPath(BigDecimal.class, trxnAmountVal.getMetadata());

        SQLQuery productQuery = factory.query().select(accountId, periodId, amountVal.as(AMOUNT_VAL)).from(tablePath) //
                .where(productFilter);

        SQLQuery revenueSubQuery = factory.query()
                .select(keysAccountId, keysPeriodId, trxnAmount.sum().coalesce(BigDecimal.ZERO).as(REVENUE))
                .from(keysPath).leftJoin(productQuery, trxnPath)
                .on(keysAccountId.eq(trxnAccountId).and(keysPeriodId.eq(trxnPeriodId)))
                .groupBy(keysAccountId, keysPeriodId);

        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(revenueSubQuery);
        subQuery.setAlias(TRXN_REVENUE);
        return subQuery.withProjection(ACCOUNT_ID).withProjection(PERIOD_ID).withProjection(REVENUE);
    }

    @SuppressWarnings("unchecked")
    private SubQuery translateShiftedRevenue(QueryFactory queryFactory, AttributeRepository repository,
            int laggingPeriodCount, String sqlUser) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);
        NumberExpression revenueNumber = Expressions.numberPath(BigDecimal.class, revenueRevenue.getMetadata());
        WindowFunction windowAgg = SQLExpressions.sum(revenueNumber.coalesce(BigDecimal.ZERO)).over()
                .partitionBy(revenueAccountId).orderBy(revenuePeriodId).rows().between()
                .following(ONE_LAG_BEHIND_OFFSET).following(laggingPeriodCount);

        SQLQuery revenueSubQuery = factory.query() //
                .select(revenueAccountId, revenuePeriodId, windowAgg.as(REVENUE)) //
                .from(revenuePath);

        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(revenueSubQuery);
        subQuery.setAlias(SHIFTED_REVENUE);
        return subQuery.withProjection(ACCOUNT_ID).withProjection(PERIOD_ID).withProjection(REVENUE);
    }

    @SuppressWarnings("unchecked")
    private Expression translateEvaluationPeriodId(QueryFactory queryFactory, AttributeRepository repository,
            int evaluationPeriodId, String sqlUser) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);
        StringPath tablePath = Expressions.stringPath(TEMP_TRXN);
        NumberPath periodId = Expressions.numberPath(Integer.class, tablePath, PERIOD_ID);

        if (evaluationPeriodId < 0) {
            return factory.query().from(tablePath).select(periodId.max().as(MAX_PID));
        } else {
            return Expressions.asNumber(evaluationPeriodId);
        }
    }

    @SuppressWarnings("unchecked")
    private Expression translateMaxTrainingPeriodId(QueryFactory queryFactory, AttributeRepository repository,
            int evaluationPeriodId, int laggingPeriodCount, String sqlUser) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);
        StringPath tablePath = Expressions.stringPath(TEMP_TRXN);
        NumberPath periodId = Expressions.numberPath(Integer.class, tablePath, PERIOD_ID);

        if (evaluationPeriodId < 0) {
            return factory.query().from(tablePath).select(periodId.max().subtract(laggingPeriodCount));
        } else {
            return Expressions.asNumber(evaluationPeriodId - laggingPeriodCount);
        }
    }

    @SuppressWarnings("unchecked")
    private Expression translateMinPeriodId(QueryFactory queryFactory, AttributeRepository repository,
            int evaluationPeriodId, int laggingPeriodCount, int periodCount, String sqlUser) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);
        StringPath tablePath = Expressions.stringPath(TEMP_TRXN);
        NumberPath periodId = Expressions.numberPath(Integer.class, tablePath, PERIOD_ID);

        if (periodCount <= 0) {
            return Expressions.constant(0);
        } else {
            if (evaluationPeriodId < 0) {
                return factory.query().from(tablePath)
                        .select(periodId.max().subtract(laggingPeriodCount).subtract(periodCount));
            } else {
                return Expressions.constant(String.valueOf(evaluationPeriodId - laggingPeriodCount - periodCount));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Expression translateMinPeriodId(QueryFactory queryFactory, AttributeRepository repository, int offset,
            String sqlUser) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);
        StringPath tablePath = Expressions.stringPath(TEMP_TRXN);
        NumberPath periodId = Expressions.numberPath(Integer.class, tablePath, PERIOD_ID);

        return factory.query().from(tablePath).select(periodId.min().add(offset));
    }

    private BooleanExpression translateProductId(String productIdStr) {
        return productId.in(productIdStr.split(","));
    }

    @SuppressWarnings("unchecked")
    private BooleanExpression translatePeriodRestriction(QueryFactory queryFactory, AttributeRepository repository,
            boolean isScoring, int evaluationPeriodId, int laggingPeriodCount, String sqlUser) {
        return isScoring //
                ? periodId.eq(translateEvaluationPeriodId(queryFactory, repository, evaluationPeriodId, sqlUser)) //
                : periodId.loe(translateMaxTrainingPeriodId(queryFactory, repository, evaluationPeriodId,
                        laggingPeriodCount, sqlUser));
    }

    private SQLQuery joinAccountWithPeriods(QueryFactory queryFactory, AttributeRepository repository,
            String accountViewAlias, int evaluationPeriodId, int laggingPeriodCount, boolean isScoring,
            String sqlUser) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);

        EntityPath<String> accountViewPath = new PathBuilder<>(String.class, accountViewAlias);
        StringPath qualifiedAccountId = Expressions.stringPath(accountViewPath, ACCOUNT_ID);
        BooleanExpression periodIdPredicate = translatePeriodRestriction(queryFactory, repository, isScoring,
                evaluationPeriodId, laggingPeriodCount, sqlUser);
        return factory.query().select(keysAccountId, keysPeriodId).from(keysPath).join(accountViewPath)
                .on(keysAccountId.eq(qualifiedAccountId)).where(periodIdPredicate);
    }

    private SQLQuery joinEventTupleWithRevenue(QueryFactory queryFactory, AttributeRepository repository,
            String eventTupleViewAlias, String sqlUser) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);

        EntityPath<String> eventViewPath = new PathBuilder<>(String.class, eventTupleViewAlias);
        StringPath tupleAccountId = Expressions.stringPath(eventViewPath, ACCOUNT_ID);
        StringPath tuplePeriodId = Expressions.stringPath(eventViewPath, PERIOD_ID);
        return factory.query().select(shiftedAccountId, shiftedPeriodId, shiftedRevenue).from(shiftedRevenuePath)
                .join(eventViewPath).on(tupleAccountId.eq(shiftedAccountId).and(tuplePeriodId.eq(shiftedPeriodId)));
    }

    @SuppressWarnings("unchecked")
    private SubQuery translateSingleProductAPS(QueryFactory queryFactory, AttributeRepository repository,
            TransactionRestriction txRestriction, int evaluationPeriodId, int laggingPeriodCount, boolean isScoring,
            String sqlUser) {

        TimeFilter timeFilter = txRestriction.getTimeFilter();
        AggregationFilter spentFilter = txRestriction.getSpentFilter();
        AggregationFilter unitFilter = txRestriction.getUnitFilter();

        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);
        StringPath tablePath = Expressions.stringPath(TEMP_TRXN);

        List<Expression> productSelectList = new ArrayList(Arrays.asList(accountId, periodId));
        List<Expression> apsSelectList = new ArrayList(Arrays.asList(keysAccountId, keysPeriodId));

        productSelectList.add(amountVal.as(AMOUNT_VAL));
        productSelectList.add(quantityVal.as(QUANTITY_VAL));

        if (spentFilter != null) {
            Expression spentWindowAgg = translateAggregateTimeWindow(keysAccountId, keysPeriodId, trxnAmountVal,
                    timeFilter, spentFilter, true).as(amountAggr);
            apsSelectList.add(spentWindowAgg);
        } else {
            apsSelectList.add(SQLExpressions.selectZero().as(amountAggr));
        }

        if (unitFilter != null) {
            Expression unitWindowAgg = translateAggregateTimeWindow(keysAccountId, keysPeriodId, trxnQuantityVal,
                    timeFilter, unitFilter, true).as(quantityAggr);
            apsSelectList.add(unitWindowAgg);
        } else {
            apsSelectList.add(SQLExpressions.selectZero().as(quantityAggr));
        }

        SQLQuery productQuery = factory.query().select(productSelectList.toArray(new Expression[0])).from(tablePath)
                .where(translateProductId(txRestriction.getProductId()));

        SQLQuery apsQuery = factory.query().select(apsSelectList.toArray(new Expression[0])).from(keysPath)
                .leftJoin(productQuery, trxnPath)
                .on(keysAccountId.eq(trxnAccountId).and(keysPeriodId.eq(trxnPeriodId)));

        BooleanExpression periodIdPredicate = translatePeriodRestriction(queryFactory, repository, isScoring,
                evaluationPeriodId, laggingPeriodCount, sqlUser);

        if (txRestriction.isSkipOffset()) {
            int offset = Integer.valueOf(timeFilter.getValues().get(0).toString());

            periodIdPredicate = periodIdPredicate
                    .and(periodId.gt(translateMinPeriodId(queryFactory, repository, offset, sqlUser)));
        }

        SQLQuery subQueryExpression = factory.query().select(accountId, periodId, amountAggr, quantityAggr) //
                .from(apsQuery, apsPath) //
                .where(periodIdPredicate);

        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(subQueryExpression);
        subQuery.setAlias(generateAlias(APS));
        return subQuery.withProjections(ACCOUNT_ID, PERIOD_ID, AMOUNT_AGG, QUANTITY_AGG);
    }

    private SQLQuery translateMultiProductRestriction(QueryFactory queryFactory, AttributeRepository repository,
            TransactionRestriction txOld, int evaluationPeriodId, int laggingPeriodCount, boolean isScoring,
            QueryBuilder builder, String sqlUser) {

        String[] products = txOld.getProductId().split(",");

        SubQuery[] apsSubQueryList = Stream.of(products).map(product -> {
            TransactionRestriction txNew = new TransactionRestriction(product, txOld.getTimeFilter(), txOld.isNegate(),
                    txOld.getSpentFilter(), txOld.getUnitFilter());
            return translateSingleProductAPS(queryFactory, repository, txNew, evaluationPeriodId, laggingPeriodCount,
                    isScoring, sqlUser);
        }).toArray(SubQuery[]::new);

        builder.with(apsSubQueryList);

        SubQuery apsUnionAll = translateAPSUnionAll(queryFactory, repository, apsSubQueryList, sqlUser);

        builder.with(apsUnionAll);

        SubQuery apsUnionAllNoNull = translateAPSUnionAllReplaceNull(queryFactory, repository, apsUnionAll.getAlias(),
                sqlUser);

        builder.with(apsUnionAllNoNull);

        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);

        EntityPath<String> apsUnionAllPath = (products.length == 1) //
                ? new PathBuilder<>(String.class, apsUnionAll.getAlias()) //
                : new PathBuilder<>(String.class, apsUnionAllNoNull.getAlias());

        AggregationFilter spentFilter = txOld.getSpentFilter();
        AggregationFilter unitFilter = txOld.getUnitFilter();

        BooleanExpression aggrAmountPredicate = (spentFilter != null)
                ? translateAggregatePredicate(amountAggr, spentFilter, true)
                : Expressions.TRUE;
        BooleanExpression aggrQuantityPredicate = (unitFilter != null)
                ? translateAggregatePredicate(quantityAggr, unitFilter, true)
                : Expressions.TRUE;

        BooleanExpression aggrValPredicate = aggrAmountPredicate.and(aggrQuantityPredicate);

        if (txOld.isNegate()) {
            aggrValPredicate = aggrValPredicate.not();
        }

        return factory.query().select(accountId, periodId).from(apsUnionAllPath) //
                .groupBy(accountId, periodId).having(aggrValPredicate);
    }

    private SQLQuery translateTransaction(QueryFactory queryFactory, AttributeRepository repository,
            TransactionRestriction txRestriction, int evaluationPeriodId, int laggingPeriodCount, boolean isScoring,
            QueryBuilder builder, String sqlUser) {

        if (txRestriction.getSpentFilter() == null && txRestriction.getUnitFilter() == null) {
            return translateHasEngaged(queryFactory, repository, txRestriction, evaluationPeriodId, laggingPeriodCount,
                    isScoring, sqlUser);
        }

        return translateMultiProductRestriction(queryFactory, repository, txRestriction, evaluationPeriodId,
                laggingPeriodCount, isScoring, builder, sqlUser);
    }

    @SuppressWarnings("unchecked")
    private SQLQuery translateHasEngaged(QueryFactory queryFactory, AttributeRepository repository,
            TransactionRestriction txRestriction, int evaluationPeriodId, int laggingPeriodCount, boolean isScoring,
            String sqlUser) {

        TimeFilter timeFilter = txRestriction.getTimeFilter();
        String productIdStr = txRestriction.getProductId();
        boolean returnPositive = !txRestriction.isNegate();

        SQLQueryFactory factory = queryFactory.getSQLQueryFactory(repository, sqlUser);
        StringPath tablePath = Expressions.stringPath(TEMP_TRXN);

        NumberExpression trxnAmountValNumber = Expressions.numberPath(BigDecimal.class, trxnAmountVal.getMetadata());
        NumberExpression trxnQuantityValNumber = Expressions.numberPath(BigDecimal.class,
                trxnQuantityVal.getMetadata());
        CaseBuilder caseBuilder = new CaseBuilder();
        NumberExpression trxnValExists = caseBuilder.when(trxnAmountValNumber.gt(0)).then(1)
                .when(trxnQuantityValNumber.gt(0)).then(1).otherwise(0);

        Expression windowAgg = translateTimeWindow(timeFilter,
                SQLExpressions.max(trxnValExists).over().partitionBy(keysAccountId).orderBy(keysPeriodId))
                        .as(amountAggr);

        SQLQuery productQuery = factory.query() //
                .select(accountId, periodId, amountVal.as(AMOUNT_VAL), quantityVal.as(QUANTITY_VAL)).from(tablePath) //
                .where(translateProductId(productIdStr));

        SQLQuery apsQuery = factory.query().select(keysAccountId, keysPeriodId, trxnAmountVal, windowAgg).from(keysPath)
                .leftJoin(productQuery, trxnPath)
                .on(keysAccountId.eq(trxnAccountId).and(keysPeriodId.eq(trxnPeriodId)));

        BooleanExpression periodIdPredicate = translatePeriodRestriction(queryFactory, repository, isScoring,
                evaluationPeriodId, laggingPeriodCount, sqlUser);

        int expectedResult = (returnPositive) ? 1 : 0;

        BooleanExpression resultFilter = amountAggr.eq(String.valueOf(expectedResult)).and(periodIdPredicate);

        if (txRestriction.isSkipOffset()) {
            int offset = Integer.valueOf(timeFilter.getValues().get(0).toString());

            resultFilter = resultFilter
                    .and(periodId.gt(translateMinPeriodId(queryFactory, repository, offset, sqlUser)));
        }

        return factory.query().select(accountId, periodId).from(apsQuery, apsPath).where(resultFilter)
                .groupBy(accountId, periodId);

    }

    private SubQuery translateSelectAll(QueryFactory queryFactory, AttributeRepository repository, String tableName,
            String sqlUser) {
        SQLQueryFactory factory = queryFactory.getSQLQueryFactory(repository, sqlUser);
        EntityPath<String> tablePath = new PathBuilder<>(String.class, tableName);
        SQLQuery selectAll = factory.query().select(SQLExpressions.all).from(tablePath);
        SubQuery query = new SubQuery();
        query.setSubQueryExpression(selectAll);
        query.setAlias(tableName);
        return query;
    }

    @SuppressWarnings("unchecked")
    private SubQuery translateSelectAll(QueryFactory queryFactory, AttributeRepository repository, String tableName,
            int evaluationPeriodId, int laggingPeriodCount, int periodCount, String sqlUser, boolean hasSegmentQuery) {
        SQLQueryFactory factory = queryFactory.getSQLQueryFactory(repository, sqlUser);
        EntityPath<String> tablePath = new PathBuilder<>(String.class, tableName);
        BooleanExpression periodRestriction = periodId.gt(translateMinPeriodId(queryFactory, repository,
                evaluationPeriodId, laggingPeriodCount, periodCount, sqlUser));
        SQLQuery selectAll;
        if (hasSegmentQuery) {
            EntityPath<String> segmentPath = new PathBuilder<>(String.class, SEGMENT);
            StringPath tableId = Expressions.stringPath(tablePath, InterfaceName.AccountId.name());
            StringPath segmentId = Expressions.stringPath(segmentPath, InterfaceName.AccountId.name());
            selectAll = factory.query() //
                    .select(SQLExpressions.all).from(tablePath) //
                    .join(segmentPath).on(tableId.eq(segmentId)) //
                    .where(periodRestriction);
        } else {
            selectAll = factory.query() //
                    .select(SQLExpressions.all).from(tablePath) //
                    .where(periodRestriction);

        }
        SubQuery query = new SubQuery();
        query.setSubQueryExpression(selectAll);
        query.setAlias(tableName);
        return query;
    }

    private SubQuery translateEventRevenue(QueryFactory queryFactory, AttributeRepository repository,
            String eventTupleViewAlias, String sqlUser) {
        SQLQuery selectAll = joinEventTupleWithRevenue(queryFactory, repository, eventTupleViewAlias, sqlUser);
        SubQuery query = new SubQuery();
        query.setSubQueryExpression(selectAll);
        query.setAlias(generateAlias("Revenue"));
        return query.withProjection(ACCOUNT_ID).withProjection(PERIOD_ID).withProjection(REVENUE);
    }

    private SubQuery translateTransactionRestriction(QueryFactory queryFactory, AttributeRepository repository,
            TransactionRestriction txRestriction, int evaluationPeriodId, int laggingPeriodCount, boolean isScoring,
            QueryBuilder builder, String sqlUser) {
        if (StringUtils.isEmpty(txRestriction.getProductId())) {
            throw new RuntimeException("Invalid transaction restriction, no product specified");
        }
        SQLQuery subQueryExpression = translateTransaction(queryFactory, repository, txRestriction, evaluationPeriodId,
                laggingPeriodCount, isScoring, builder, sqlUser);
        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(subQueryExpression);
        subQuery.setAlias(generateAlias(BusinessEntity.Transaction.name()));
        return subQuery.withProjection(ACCOUNT_ID).withProjection(PERIOD_ID);
    }

    private SubQuery translateAccountViewWithJoinedPeriods(QueryFactory queryFactory, AttributeRepository repository,
            String accountViewAlias, int evaluationPeriodId, int laggingPeriodCount, boolean isScoring,
            String sqlUser) {
        SQLQuery subQueryExpression = joinAccountWithPeriods(queryFactory, repository, accountViewAlias,
                evaluationPeriodId, laggingPeriodCount, isScoring, sqlUser);
        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(subQueryExpression);
        subQuery.setAlias(generateAlias(BusinessEntity.Account.name()));
        return subQuery.withProjection(ACCOUNT_ID).withProjection(PERIOD_ID);
    }

    @SuppressWarnings("unchecked")
    private SubQuery translateTransactionPeriodId(QueryFactory queryFactory, //
            AttributeRepository repository, String sqlUser) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);
        NumberPath periodId = Expressions.numberPath(Integer.class, PERIOD_ID);
        Expression subQueryExpression = factory.query().select(periodId.max()).from(Expressions.stringPath(TEMP_TRXN));
        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(subQueryExpression);
        subQuery.setAlias(MAX_PID);
        return subQuery.withProjection(PERIOD_ID);
    }

    private SubQuery translateDistinctAccounts(QueryFactory queryFactory, //
            AttributeRepository repository, String sqlUser, int evaluationPeriodId) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);
        EntityPath<String> accountViewPath = new PathBuilder<>(String.class, KEYS);
        StringPath accountId = Expressions.stringPath(accountViewPath, ACCOUNT_ID);

        SQLQuery subQueryExpression;
        if (evaluationPeriodId < 0) {
            EntityPath<String> periodViewPath = new PathBuilder<>(String.class, MAX_PID);
            NumberExpression periodIdExpression = Expressions.numberPath(Long.class, periodViewPath, PERIOD_ID);
            subQueryExpression = factory.query().distinct().select(accountId, periodIdExpression)
                    .from(keysPath, Expressions.stringPath(MAX_PID));
        } else {
            NumberExpression periodIdExpression = Expressions.asNumber(evaluationPeriodId).as(PERIOD_ID);
            subQueryExpression = factory.query().distinct().select(accountId, periodIdExpression)
                    .from(keysPath);
        }
        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(subQueryExpression);
        subQuery.setAlias(generateAlias(BusinessEntity.Account.name()));
        return subQuery.withProjection(ACCOUNT_ID).withProjection(PERIOD_ID);
    }

    private SubQuery translateAccountView(ConcreteRestriction restriction) {

        AttributeLookup accountId = new AttributeLookup(BusinessEntity.Account, ACCOUNT_ID);

        Query accountQuery = Query.builder().select(accountId).from(BusinessEntity.Account).where(restriction).build();

        SubQuery subQuery = new SubQuery(accountQuery, generateAlias(BusinessEntity.Account.name()));
        return subQuery.withProjection(ACCOUNT_ID);
    }

    private TransactionRestriction translateToPrior(TransactionRestriction priorOnly) {

        TimeFilter timeFilter = new TimeFilter(priorOnly.getTimeFilter().getLhs(), ComparisonType.PRIOR, //
                priorOnly.getTimeFilter().getPeriod(), priorOnly.getTimeFilter().getValues());
        return new TransactionRestriction(priorOnly.getProductId(), //
                timeFilter, //
                false, //
                priorOnly.getSpentFilter(), //
                priorOnly.getUnitFilter());
    }

    private TransactionRestriction translateToNotEngagedWithin(TransactionRestriction priorOnly) {

        TimeFilter timeFilter = new TimeFilter(priorOnly.getTimeFilter().getLhs(), ComparisonType.WITHIN, //
                priorOnly.getTimeFilter().getPeriod(), priorOnly.getTimeFilter().getValues());

        return new TransactionRestriction(priorOnly.getProductId(), //
                timeFilter, //
                true, //
                null, //
                null);
    }

    private TransactionRestriction translateToEngagedWithin(TransactionRestriction priorOnly, boolean skipOffset) {

        TimeFilter timeFilter = new TimeFilter(priorOnly.getTimeFilter().getLhs(), ComparisonType.WITHIN, //
                priorOnly.getTimeFilter().getPeriod(), priorOnly.getTimeFilter().getValues());

        return new TransactionRestriction(priorOnly.getProductId(), //
                timeFilter, //
                false, //
                null, //
                null, skipOffset);
    }

    private TransactionRestriction translateToNotEngagedEver(TransactionRestriction priorOnly, boolean skipOffset) {

        TimeFilter timeFilter = new TimeFilter(priorOnly.getTimeFilter().getLhs(), ComparisonType.EVER, //
                priorOnly.getTimeFilter().getPeriod(), priorOnly.getTimeFilter().getValues());

        return new TransactionRestriction(priorOnly.getProductId(), //
                timeFilter, //
                true, //
                null, //
                null, skipOffset);
    }

    private boolean calculateEventRevenue(boolean isEvent, EventFrontEndQuery frontEndQuery) {
        return isEvent && frontEndQuery.getCalculateProductRevenue();
    }

    private QueryBuilder translateRestriction(QueryFactory queryFactory, AttributeRepository repository,
            Restriction restriction, boolean isScoring, boolean isEvent, EventFrontEndQuery frontEndQuery,
            QueryBuilder builder, String sqlUser) {

        final String period = frontEndQuery.getPeriodName();
        final int periodCount = frontEndQuery.getPeriodCount();
        final int laggingPeriodCount = frontEndQuery.getLaggingPeriodCount();
        final int evaluationPeriodId = frontEndQuery.getEvaluationPeriodId();

        if (laggingPeriodCount < ONE_LAG_BEHIND_OFFSET) {
            throw new IllegalArgumentException("Invalid lagging period count: " + laggingPeriodCount);
        }

        if (StringUtils.isBlank(period)) {
            throw new IllegalArgumentException(
                    "Must specify a period name in front end query: " + JsonUtils.serialize(frontEndQuery));
        }

        Map<LogicalRestriction, List<String>> subQueryTableMap = new HashMap<>();
        Restriction rootRestriction = restriction;
        overwritePeriodsInRestriction(rootRestriction, period);
        rootRestriction = RestrictionOptimizer.optimize(rootRestriction);

        if (!SparkQueryProvider.SPARK_BATCH_USER.equals(sqlUser)) {
            // Spark SQL prepares these two temp views before-hand.
            builder.with(translateTempTrxn(queryFactory, repository, period, sqlUser));
            builder.with(translateAllKeys(queryFactory, repository, sqlUser));
        }
        if (frontEndQuery.getSegmentSubQuery() != null) {
            builder.with(translateSegment(frontEndQuery.getSegmentSubQuery()));
        }

        if (calculateEventRevenue(isEvent, frontEndQuery)) {
            if (frontEndQuery.getTargetProductIds() == null || frontEndQuery.getTargetProductIds().isEmpty()) {
                throw new RuntimeException("Fail to calculate product revenue. No target product specified");
            }
            builder.with(
                    translateProductRevenue(queryFactory, repository, frontEndQuery.getTargetProductIds(), sqlUser));
            builder.with(translateShiftedRevenue(queryFactory, repository, laggingPeriodCount, sqlUser));
        }

        // special handling for less operation to exclude "not purchased"
        if (rootRestriction instanceof LogicalRestriction) {
            BreadthFirstSearch bfs = new BreadthFirstSearch();
            bfs.run(rootRestriction, (object, ctx) -> {
                if (object instanceof TransactionRestriction) {
                    TransactionRestriction txRestriction = (TransactionRestriction) object;
                    if (excludeNotPurchased(txRestriction)) {
                        Restriction newRestriction = translateExcludeNotPurchased(txRestriction);
                        LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                        parent.getRestrictions().remove(txRestriction);
                        parent.getRestrictions().add(newRestriction);
                    }
                }
            });
        } else if (rootRestriction instanceof TransactionRestriction) {
            TransactionRestriction txRestriction = (TransactionRestriction) rootRestriction;

            if (excludeNotPurchased(txRestriction)) {
                rootRestriction = translateExcludeNotPurchased(txRestriction);
            }
        }

        // translate mutli-product "has engaged" to logical grouping
        if (rootRestriction instanceof LogicalRestriction) {
            BreadthFirstSearch bfs = new BreadthFirstSearch();
            bfs.run(rootRestriction, (object, ctx) -> {
                if (object instanceof TransactionRestriction) {
                    TransactionRestriction txRestriction = (TransactionRestriction) object;
                    if (isHasEngagedRestriction(txRestriction)) {
                        Restriction newRestriction = translateHasEngagedToLogicalGroup(txRestriction);
                        LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                        parent.getRestrictions().remove(txRestriction);
                        parent.getRestrictions().add(newRestriction);
                    }
                }
            });
        } else if (rootRestriction instanceof TransactionRestriction) {
            TransactionRestriction txRestriction = (TransactionRestriction) rootRestriction;

            if (isHasEngagedRestriction(txRestriction)) {
                rootRestriction = translateHasEngagedToLogicalGroup(txRestriction);
            }
        }

        // special treatment for NOT WITHIN to skip offset
        if (rootRestriction instanceof LogicalRestriction) {
            BreadthFirstSearch bfs = new BreadthFirstSearch();
            bfs.run(rootRestriction, (object, ctx) -> {
                if (object instanceof TransactionRestriction) {
                    TransactionRestriction txRestriction = (TransactionRestriction) object;
                    if (txRestriction.isNegate()
                            && ComparisonType.WITHIN == txRestriction.getTimeFilter().getRelation()) {
                        Restriction newRestriction = translateNotWithinToSkipOffset(txRestriction);
                        LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                        parent.getRestrictions().remove(txRestriction);
                        parent.getRestrictions().add(newRestriction);
                    }
                }
            });
        } else if (rootRestriction instanceof TransactionRestriction) {
            TransactionRestriction txRestriction = (TransactionRestriction) rootRestriction;
            if (txRestriction.isNegate() && ComparisonType.WITHIN == txRestriction.getTimeFilter().getRelation()) {
                rootRestriction = translateNotWithinToSkipOffset(txRestriction);
            }
        }

        // special treatment for PRIOR_ONLY
        if (rootRestriction instanceof LogicalRestriction) {
            BreadthFirstSearch bfs = new BreadthFirstSearch();
            bfs.run(rootRestriction, (object, ctx) -> {
                if (object instanceof TransactionRestriction) {
                    TransactionRestriction txRestriction = (TransactionRestriction) object;
                    if (ComparisonType.PRIOR_ONLY == txRestriction.getTimeFilter().getRelation()) {
                        Restriction newRestriction = translatePriorOnly(txRestriction);
                        LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                        parent.getRestrictions().remove(txRestriction);
                        parent.getRestrictions().add(newRestriction);
                    }
                }
            });
        } else if (rootRestriction instanceof TransactionRestriction) {
            TransactionRestriction txRestriction = (TransactionRestriction) rootRestriction;
            if (ComparisonType.PRIOR_ONLY == txRestriction.getTimeFilter().getRelation()) {
                rootRestriction = translatePriorOnly(txRestriction);
            }
        }

        // translate restrictions to individual sub-queries
        AtomicReference<String> finalAliasRef = new AtomicReference<>("");
        if (rootRestriction instanceof LogicalRestriction) {
            BreadthFirstSearch bfs = new BreadthFirstSearch();
            bfs.run(rootRestriction, (object, ctx) -> {
                if (object instanceof LogicalRestriction) {
                    LogicalRestriction logicalRestriction = (LogicalRestriction) object;
                    subQueryTableMap.put(logicalRestriction, new ArrayList<>());
                } else if (object instanceof TransactionRestriction) {
                    TransactionRestriction txRestriction = (TransactionRestriction) object;
                    SubQuery subQuery = translateTransactionRestriction(queryFactory, repository, txRestriction,
                            evaluationPeriodId, laggingPeriodCount, isScoring, builder, sqlUser);
                    builder.with(subQuery);
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    List<String> childSubQueryList = subQueryTableMap.get(parent);
                    childSubQueryList.add(subQuery.getAlias());
                } else if (object instanceof ConcreteRestriction) {
                    ConcreteRestriction concreteRestriction = (ConcreteRestriction) object;
                    SubQuery accountViewSubquery = translateAccountView(concreteRestriction);
                    builder.with(accountViewSubquery);
                    SubQuery subQuery = translateAccountViewWithJoinedPeriods(queryFactory, repository,
                            accountViewSubquery.getAlias(), evaluationPeriodId, laggingPeriodCount, isScoring, sqlUser);
                    builder.with(subQuery);
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    List<String> childSubQueryList = subQueryTableMap.get(parent);
                    childSubQueryList.add(subQuery.getAlias());
                }
            });
        } else if (rootRestriction instanceof TransactionRestriction) {
            TransactionRestriction txRestriction = (TransactionRestriction) rootRestriction;
            SubQuery subQuery = translateTransactionRestriction(queryFactory, repository, txRestriction,
                    evaluationPeriodId, laggingPeriodCount, isScoring, builder, sqlUser);
            builder.with(subQuery);
            finalAliasRef.set(subQuery.getAlias());
        } else if (rootRestriction instanceof ConcreteRestriction) {
            ConcreteRestriction concreteRestriction = (ConcreteRestriction) rootRestriction;
            SubQuery accountViewSubquery = translateAccountView(concreteRestriction);
            builder.with(accountViewSubquery);
            SubQuery subQuery = translateAccountViewWithJoinedPeriods(queryFactory, repository,
                    accountViewSubquery.getAlias(), evaluationPeriodId, laggingPeriodCount, isScoring, sqlUser);
            builder.with(subQuery);
            String selectAllAlias;
            if (calculateEventRevenue(isEvent, frontEndQuery)) {
                SubQuery revenueSubQuery = translateEventRevenue(queryFactory, repository, subQuery.getAlias(),
                        sqlUser);
                builder.with(revenueSubQuery);
                selectAllAlias = revenueSubQuery.getAlias();
            } else {
                selectAllAlias = subQuery.getAlias();
            }
            finalAliasRef.set(selectAllAlias);
        } else if (rootRestriction == null) {
            if (evaluationPeriodId < 0) {
                SubQuery trxnPeriod = translateTransactionPeriodId(queryFactory, repository, sqlUser);
                builder.with(trxnPeriod);
            }
            SubQuery distinctAccounts = //
                    translateDistinctAccounts(queryFactory, repository, sqlUser, evaluationPeriodId);
            builder.with(distinctAccounts);
            finalAliasRef.set(distinctAccounts.getAlias());
        } else {
            throw new UnsupportedOperationException("Cannot translate restriction " + restriction);
        }

        // merge sub queries into final query
        if (!subQueryTableMap.isEmpty()) {
            Map<LogicalRestriction, UnionCollector> unionMap = new HashMap<>();

            for (LogicalRestriction logicalRestriction : subQueryTableMap.keySet()) {
                List<String> childLookups = subQueryTableMap.get(logicalRestriction);
                // process leaf nodes
                if (childLookups != null && childLookups.size() != 0) {
                    SQLQuery[] childQueries = childLookups.stream() //
                            .map(x -> (SQLQuery) translateSelectAll(queryFactory, repository, x, sqlUser) //
                                    .getSubQueryExpression()) //
                            .toArray(SQLQuery[]::new);

                    Union<?> union = mergeQueryResult(logicalRestriction.getOperator(), childQueries);
                    UnionCollector collector = unionMap.getOrDefault(logicalRestriction, new UnionCollector());
                    unionMap.put(logicalRestriction, collector.withUnion(union));
                }
            }

            DepthFirstSearch dfs = new DepthFirstSearch();
            dfs.run(rootRestriction, (object, ctx) -> {
                if (object instanceof LogicalRestriction) {
                    LogicalRestriction logicalRestriction = (LogicalRestriction) object;
                    UnionCollector collector = unionMap.get(logicalRestriction);

                    if (collector == null || collector.size() == 0) {
                        // ignore or log error since this shouldn't happen
                        return;
                    }

                    SubQueryExpression merged;
                    if (collector.size() > 1) {
                        merged = mergeQueryResult(logicalRestriction.getOperator(), collector.asArray());
                        collector.reset(merged);
                    } else {
                        merged = collector.asList().get(0);
                    }

                    // generate CTE for intermediary tables
                    // todo: experiment with skipping intermediate tables to see if it improves
                    // performance
                    String mergedTableAlias = generateAlias("Logical");
                    SubQuery mergedQuery = new SubQuery();
                    mergedQuery.setSubQueryExpression(merged);
                    mergedQuery.setAlias(mergedTableAlias);
                    mergedQuery.withProjection(ACCOUNT_ID).withProjection(PERIOD_ID);
                    builder.with(mergedQuery);

                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    if (parent != null) {
                        SQLQuery childQuery = (SQLQuery) translateSelectAll(queryFactory, repository, mergedTableAlias,
                                sqlUser).getSubQueryExpression();

                        UnionCollector parentCollector = unionMap.getOrDefault(parent, new UnionCollector());
                        unionMap.put(parent, parentCollector.withUnion(childQuery));
                    } else {
                        String selectAllAlias;
                        if (calculateEventRevenue(isEvent, frontEndQuery)) {
                            SubQuery revenueSubQuery = translateEventRevenue(queryFactory, repository, mergedTableAlias,
                                    sqlUser);
                            builder.with(revenueSubQuery);
                            selectAllAlias = revenueSubQuery.getAlias();
                        } else {
                            selectAllAlias = mergedTableAlias;
                        }
                        finalAliasRef.set(selectAllAlias);
                    }
                }
            }, true);
        }

        // final "select ... from ..." statement
        if (StringUtils.isBlank(finalAliasRef.get())) {
            throw new IllegalStateException("Cannot resolve final alias from restriction " + restriction);
        }
        SubQuery selectAll = translateSelectAll(queryFactory, repository, finalAliasRef.get(), evaluationPeriodId,
                laggingPeriodCount, periodCount, sqlUser, frontEndQuery.getSegmentQuery() != null);
        SubQueryAttrLookup accountId = new SubQueryAttrLookup(selectAll, ACCOUNT_ID);
        SubQueryAttrLookup periodId = new SubQueryAttrLookup(selectAll, PERIOD_ID);
        builder.from(selectAll);
        builder.select(accountId, periodId);
        if (calculateEventRevenue(isEvent, frontEndQuery)) {
            SubQueryAttrLookup revenue = new SubQueryAttrLookup(selectAll, REVENUE);
            builder.select(revenue);
        }

        return builder;
    }

    private void overwritePeriodsInRestriction(Restriction rootRestriction, final String period) {
        // set transaction entity
        if (rootRestriction instanceof LogicalRestriction) {
            BreadthFirstSearch bfs = new BreadthFirstSearch();
            bfs.run(rootRestriction, (object, ctx) -> {
                if (object instanceof TransactionRestriction) {
                    TransactionRestriction txRestriction = (TransactionRestriction) object;
                    overwritePeriod(txRestriction, period);
                }
            });
        } else if (rootRestriction instanceof TransactionRestriction) {
            TransactionRestriction txRestriction = (TransactionRestriction) rootRestriction;
            overwritePeriod(txRestriction, period);
        }
    }

    private synchronized void overwritePeriod(TransactionRestriction txRestriction, String period) {
        TimeFilter timeFilter = txRestriction.getTimeFilter();
        if (timeFilter != null) {
            timeFilter.setPeriod(period);
        }
    }

    private Restriction translatePriorOnly(TransactionRestriction txRestriction) {
        Restriction newRestriction;
        if (!txRestriction.isNegate()) {
            Restriction prior = translateToPrior(txRestriction);
            Restriction notEngagedWithin = translateToNotEngagedWithin(txRestriction);
            newRestriction = Restriction.builder().and(prior, notEngagedWithin).build();
        } else {
            Restriction notEngagedEver = translateToNotEngagedEver(txRestriction, true);
            Restriction engagedWithin = translateToEngagedWithin(txRestriction, true);
            newRestriction = Restriction.builder().or(notEngagedEver, engagedWithin).build();
        }
        return newRestriction;
    }

    private Restriction translateNotWithinToSkipOffset(TransactionRestriction txRestriction) {
        return new TransactionRestriction(txRestriction.getProductId(), txRestriction.getTimeFilter(),
                txRestriction.isNegate(), txRestriction.getSpentFilter(), txRestriction.getUnitFilter(), true);
    }

    private static class UnionCollector {
        private List<SubQueryExpression> unionList = new ArrayList<>();

        UnionCollector withUnion(SubQueryExpression... unions) {
            unionList.addAll(Arrays.asList(unions));
            return this;
        }

        int size() {
            return unionList.size();
        }

        void reset(SubQueryExpression union) {
            this.unionList.clear();
            this.unionList.add(union);
        }

        List<SubQueryExpression> asList() {
            return this.unionList;
        }

        SubQueryExpression[] asArray() {
            return unionList.toArray(new SubQueryExpression[0]);
        }
    }

    @SuppressWarnings("unchecked")
    private Union<?> mergeQueryResult(LogicalOperator ops, SubQueryExpression[] childQueries) {
        return (LogicalOperator.AND == ops) //
                ? SQLExpressions.intersect(childQueries)
                : SQLExpressions.union(childQueries);
    }
}
