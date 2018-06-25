package com.latticeengines.query.exposed.translator;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AggregatedPeriodTransaction;
import static com.latticeengines.query.exposed.translator.TranslatorUtils.generateAlias;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.graph.traversal.impl.BreadthFirstSearch;
import com.latticeengines.common.exposed.graph.traversal.impl.DepthFirstSearch;
import com.latticeengines.common.exposed.util.JsonUtils;
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
import com.latticeengines.query.exposed.factory.QueryFactory;
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

    public QueryBuilder translateForScoring(QueryFactory queryFactory,
                                            AttributeRepository repository,
                                            Restriction restriction,
                                            EventFrontEndQuery frontEndQuery,
                                            QueryBuilder queryBuilder,
                                            String sqlUser) {
        return translateRestriction(queryFactory, repository, restriction, true, false, frontEndQuery, queryBuilder, sqlUser);
    }

    public QueryBuilder translateForTraining(QueryFactory queryFactory,
                                             AttributeRepository repository,
                                             Restriction restriction,
                                             EventFrontEndQuery frontEndQuery,
                                             QueryBuilder queryBuilder,
                                             String sqlUser) {
        return translateRestriction(queryFactory, repository, restriction, false, false, frontEndQuery, queryBuilder, sqlUser);
    }

    public QueryBuilder translateForEvent(QueryFactory queryFactory,
                                          AttributeRepository repository,
                                          Restriction restriction,
                                          EventFrontEndQuery frontEndQuery,
                                          QueryBuilder queryBuilder,
                                          String sqlUser) {
        return translateRestriction(queryFactory, repository, restriction, false, true, frontEndQuery, queryBuilder, sqlUser);
    }


    private SQLQueryFactory getSQLQueryFactory(QueryFactory queryFactory, AttributeRepository repository, String sqlUser) {
        return queryFactory.getSQLQueryFactory(repository, sqlUser);
    }

    protected String getPeriodTransactionTableName(AttributeRepository repository) {
        return repository.getTableName(AggregatedPeriodTransaction);
    }

    @SuppressWarnings("unchecked")
    private BooleanExpression limitPeriodByNameAndCount(QueryFactory queryFactory, AttributeRepository repository,
                                                        String period, int periodCount, String sqlUser) {
        BooleanExpression matchedPeriod = periodName.eq(period);
        return (periodCount <= 0)
            ? matchedPeriod
            : matchedPeriod.and(periodId.gt(translateValidPeriodId(queryFactory, repository, period, periodCount, sqlUser)));
    }

    @SuppressWarnings({"unchecked", "rawtype"})
    private SubQuery translateAllKeys(QueryFactory queryFactory, AttributeRepository repository, String period,
                                      int periodCount, String sqlUser) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);

        String txTableName = getPeriodTransactionTableName(repository);
        StringPath tablePath = Expressions.stringPath(txTableName);
        NumberPath minPid = Expressions.numberPath(BigDecimal.class, periodRange, MIN_PID);
        SQLQuery periodRangeSubQuery = factory.query() //
            .select(accountId, SQLExpressions.min(periodId).as(MIN_PID)) //
            .from(tablePath) //
            .where(limitPeriodByNameAndCount(queryFactory, repository, period, periodCount, sqlUser)) //
            .groupBy(accountId);

        SQLQuery crossProdQuery = factory.query().select(accountId, periodId).from(
            factory.selectDistinct(accountId).from(tablePath).where(periodName.eq(period)).as(ALL_ACCOUNTS),
            factory.selectDistinct(periodId).from(tablePath)
                .where(periodName.eq(period))
                .as(ALL_PERIODS));

        SQLQuery crossProdSubQuery = factory.query().from(crossProdQuery, crossProd)
            .innerJoin(periodRangeSubQuery, periodRange).on(periodAccountId.eq(crossAccountId))
            .where(crossPeriodId.goe(minPid.subtract(NUM_ADDITIONAL_PERIOD)));
        SQLQuery minusProdSubQuery = crossProdSubQuery.select(crossAccountId, periodId);

        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(minusProdSubQuery);
        subQuery.setAlias(KEYS);
        return subQuery.withProjection(ACCOUNT_ID).withProjection(PERIOD_ID);
    }

    @SuppressWarnings({"unchecked", "rawtype"})
    private SubQuery translateProductRevenue(QueryFactory queryFactory,
                                             AttributeRepository repository,
                                             List<String> targetProductIds,
                                             String period, String sqlUser) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);

        String txTableName = getPeriodTransactionTableName(repository);
        StringPath tablePath = Expressions.stringPath(txTableName);
        BooleanExpression periodFilter = limitPeriodByNameAndCount(queryFactory, repository, period, -1, sqlUser);
        BooleanExpression productFilter = productId.in(targetProductIds);
        NumberExpression trxnAmount = Expressions.numberPath(BigDecimal.class, trxnAmountVal.getMetadata());

        SQLQuery productQuery = factory.query()
            .select(accountId, periodId, amountVal.as(AMOUNT_VAL))
            .from(tablePath) //
            .where(periodFilter.and(productFilter));

        SQLQuery revenueSubQuery = factory.query()
            .select(keysAccountId, keysPeriodId, trxnAmount.sum().coalesce(BigDecimal.ZERO))
            .from(keysPath)
            .leftJoin(productQuery, trxnPath)
            .on(keysAccountId.eq(trxnAccountId).and(keysPeriodId.eq(trxnPeriodId)))
            .groupBy(keysAccountId, keysPeriodId);

        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(revenueSubQuery);
        subQuery.setAlias(TRXN_REVENUE);
        return subQuery.withProjection(ACCOUNT_ID).withProjection(PERIOD_ID).withProjection(REVENUE);
    }

    @SuppressWarnings({"unchecked", "rawtype"})
    private SubQuery translateShiftedRevenue(QueryFactory queryFactory,
                                             AttributeRepository repository,
                                             int laggingPeriodCount, String sqlUser) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);
        NumberExpression revenueNumber = Expressions.numberPath(BigDecimal.class, revenueRevenue.getMetadata());
        WindowFunction windowAgg = SQLExpressions.sum(revenueNumber.coalesce(BigDecimal.ZERO)).over()
            .partitionBy(revenueAccountId).orderBy(revenuePeriodId)
            .rows().between().following(ONE_LAG_BEHIND_OFFSET).following(laggingPeriodCount);

        SQLQuery revenueSubQuery = factory.query() //
            .select(revenueAccountId, revenuePeriodId, windowAgg) //
            .from(revenuePath);

        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(revenueSubQuery);
        subQuery.setAlias(SHIFTED_REVENUE);
        return subQuery.withProjection(ACCOUNT_ID).withProjection(PERIOD_ID).withProjection(REVENUE);
    }

    @SuppressWarnings("unchecked")
    private SubQueryExpression translateValidPeriodId(QueryFactory queryFactory,
                                                      AttributeRepository repository,
                                                      String period,
                                                      int periodCount,
                                                      String sqlUser) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);
        String txTableName = getPeriodTransactionTableName(repository);
        StringPath tablePath = Expressions.stringPath(txTableName);
        NumberPath periodId = Expressions.numberPath(Integer.class, tablePath, PERIOD_ID);

        return factory.query().from(tablePath) //
            .where(periodName.eq(period)) //
            .select(periodId.max().subtract(periodCount).as(MAX_PID));

    }

    @SuppressWarnings("unchecked")
    private Expression translateEvaluationPeriodId(QueryFactory queryFactory,
                                                   AttributeRepository repository,
                                                   String periodNameStr,
                                                   int evaluationPeriodId,
                                                   String sqlUser) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);
        String txTableName = getPeriodTransactionTableName(repository);
        StringPath tablePath = Expressions.stringPath(txTableName);
        NumberPath periodId = Expressions.numberPath(Integer.class, tablePath, PERIOD_ID);

        if (evaluationPeriodId < 0) {
            return factory.query().from(tablePath) //
                .where(periodName.eq(periodNameStr)) //
                .select(periodId.max().as(MAX_PID));
        } else {
            return Expressions.constant(String.valueOf(evaluationPeriodId));
        }
    }

    @SuppressWarnings("unchecked")
    private Expression translateMaxTrainingPeriodId(QueryFactory queryFactory,
                                                    AttributeRepository repository,
                                                    String periodNameStr,
                                                    int evaluationPeriodId,
                                                    int laggingPeriodCount,
                                                    String sqlUser) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);
        String txTableName = getPeriodTransactionTableName(repository);
        StringPath tablePath = Expressions.stringPath(txTableName);
        NumberPath periodId = Expressions.numberPath(Integer.class, tablePath, PERIOD_ID);

        if (evaluationPeriodId < 0) {
            return factory.query().from(tablePath) //
                .where(periodName.eq(periodNameStr)) //
                .select(periodId.max().subtract(laggingPeriodCount));
        } else {
            return Expressions.constant(String.valueOf(evaluationPeriodId - laggingPeriodCount));
        }
    }

    private BooleanExpression translateProductId(String productIdStr) {
        return productId.in(productIdStr.split(","));
    }

    @SuppressWarnings("unchecked")
    private BooleanExpression translatePeriodRestriction(QueryFactory queryFactory,
                                                         AttributeRepository repository,
                                                         boolean isScoring,
                                                         String periodName,
                                                         int evaluationPeriodId,
                                                         int laggingPeriodCount,
                                                         String sqlUser) {
        return (isScoring)
            ? periodId.eq(translateEvaluationPeriodId(queryFactory, repository, periodName, evaluationPeriodId, sqlUser))
            : periodId.loe(translateMaxTrainingPeriodId(queryFactory, repository, periodName, evaluationPeriodId,
                                                        laggingPeriodCount, sqlUser));
    }

    @SuppressWarnings("unchecked")
    private SQLQuery joinAccountWithPeriods(QueryFactory queryFactory,
                                            AttributeRepository repository,
                                            String accountViewAlias,
                                            String periodName,
                                            int evaluationPeriodId,
                                            int laggingPeriodCount,
                                            boolean isScoring,
                                            String sqlUser) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);

        EntityPath<String> accountViewPath = new PathBuilder<>(String.class, accountViewAlias);
        StringPath qualifiedAccountId = Expressions.stringPath(accountViewPath, ACCOUNT_ID);
        BooleanExpression periodIdPredicate = translatePeriodRestriction(queryFactory, repository, isScoring,
                                                                         periodName, evaluationPeriodId,
                                                                         laggingPeriodCount, sqlUser);
        return factory.query().select(keysAccountId, keysPeriodId)
            .from(keysPath)
            .join(accountViewPath)
            .on(keysAccountId.eq(qualifiedAccountId))
            .where(periodIdPredicate);
    }

    @SuppressWarnings("unchecked")
    private SQLQuery joinEventTupleWithRevenue(QueryFactory queryFactory,
                                               AttributeRepository repository,
                                               String eventTupleViewAlias,
                                               String sqlUser) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);

        EntityPath<String> eventViewPath = new PathBuilder<>(String.class, eventTupleViewAlias);
        StringPath tupleAccountId = Expressions.stringPath(eventViewPath, ACCOUNT_ID);
        StringPath tuplePeriodId = Expressions.stringPath(eventViewPath, PERIOD_ID);
        return factory.query().select(shiftedAccountId, shiftedPeriodId, shiftedRevenue)
            .from(shiftedRevenuePath)
            .join(eventViewPath)
            .on(tupleAccountId.eq(shiftedAccountId).and(tuplePeriodId.eq(shiftedPeriodId)));
    }

    @SuppressWarnings("unchecked")
    private SubQuery translateSingleProductAPS(QueryFactory queryFactory,
                                               AttributeRepository repository,
                                               TransactionRestriction txRestriction,
                                               int evaluationPeriodId,
                                               int laggingPeriodCount,
                                               boolean isScoring,
                                               String sqlUser) {

        TimeFilter timeFilter = txRestriction.getTimeFilter();
        AggregationFilter spentFilter = txRestriction.getSpentFilter();
        AggregationFilter unitFilter = txRestriction.getUnitFilter();

        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);

        String txTableName = getPeriodTransactionTableName(repository);
        StringPath tablePath = Expressions.stringPath(txTableName);

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

        String period = txRestriction.getTimeFilter().getPeriod();
        SQLQuery productQuery = factory.query()
            .select(productSelectList.toArray(new Expression[0]))
            .from(tablePath) //
            .where(periodName.eq(period)) //
            .where(translateProductId(txRestriction.getProductId()));

        SQLQuery apsQuery = factory.query()
            .select(apsSelectList.toArray(new Expression[0]))
            .from(keysPath)
            .leftJoin(productQuery, trxnPath)
            .on(keysAccountId.eq(trxnAccountId).and(keysPeriodId.eq(trxnPeriodId)));


        BooleanExpression periodIdPredicate = translatePeriodRestriction(queryFactory, repository, isScoring, period,
                                                                         evaluationPeriodId, laggingPeriodCount, sqlUser);

        SQLQuery subQueryExpression = factory.query().select(accountId, periodId, amountAggr, quantityAggr) //
            .from(apsQuery, apsPath) //
            .where(periodIdPredicate);

        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(subQueryExpression);
        subQuery.setAlias(generateAlias(APS));
        return subQuery.withProjections(ACCOUNT_ID, PERIOD_ID, AMOUNT_AGG, QUANTITY_AGG);
    }

    @SuppressWarnings("unchecked")
    private SQLQuery translateMultiProductRestriction(QueryFactory queryFactory,
                                                      AttributeRepository repository,
                                                      TransactionRestriction txOld,
                                                      int evaluationPeriodId,
                                                      int laggingPeriodCount,
                                                      boolean isScoring,
                                                      QueryBuilder builder,
                                                      String sqlUser) {

        String[] products = txOld.getProductId().split(",");

        SubQuery[] apsSubQueryList = Stream.of(products).map(product -> {
            TransactionRestriction txNew = new TransactionRestriction(product,
                                                                      txOld.getTimeFilter(),
                                                                      txOld.isNegate(),
                                                                      txOld.getSpentFilter(),
                                                                      txOld.getUnitFilter()
            );
            return translateSingleProductAPS(queryFactory, repository, txNew, evaluationPeriodId, laggingPeriodCount,
                                             isScoring, sqlUser);
        }).toArray(SubQuery[]::new);

        builder.with(apsSubQueryList);

        SubQuery apsUnionAll = translateAPSUnionAll(queryFactory, repository, apsSubQueryList, sqlUser);

        builder.with(apsUnionAll);

        SubQuery apsUnionAllNoNull = translateAPSUnionAllReplaceNull(queryFactory, repository, apsUnionAll.getAlias(), sqlUser);

        builder.with(apsUnionAllNoNull);

        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository, sqlUser);

        EntityPath<String> apsUnionAllPath = (products.length == 1) ?
            new PathBuilder<>(String.class, apsUnionAll.getAlias()) :
            new PathBuilder<>(String.class, apsUnionAllNoNull.getAlias());

        AggregationFilter spentFilter = txOld.getSpentFilter();
        AggregationFilter unitFilter = txOld.getUnitFilter();

        BooleanExpression aggrAmountPredicate =
            (spentFilter != null) ? translateAggregatePredicate(amountAggr, spentFilter, true) : Expressions.TRUE;
        BooleanExpression aggrQuantityPredicate =
            (unitFilter != null) ? translateAggregatePredicate(quantityAggr, unitFilter, true) : Expressions.TRUE;

        BooleanExpression aggrValPredicate = aggrAmountPredicate.and(aggrQuantityPredicate);

        if (txOld.isNegate()) {
            aggrValPredicate = aggrValPredicate.not();
        }

        return factory.query()
            .select(accountId, periodId)
            .from(apsUnionAllPath) //
            .groupBy(accountId, periodId)
            .having(aggrValPredicate);
    }

    @SuppressWarnings("unchecked")
    private SQLQuery translateTransaction(QueryFactory queryFactory,
                                          AttributeRepository repository,
                                          TransactionRestriction txRestriction,
                                          int evaluationPeriodId,
                                          int laggingPeriodCount,
                                          boolean isScoring,
                                          QueryBuilder builder,
                                          String sqlUser) {

        if (txRestriction.getSpentFilter() == null && txRestriction.getUnitFilter() == null) {
            return translateHasEngaged(queryFactory, repository, txRestriction, evaluationPeriodId, laggingPeriodCount,
                                       isScoring, sqlUser);
        }

        return translateMultiProductRestriction(queryFactory, repository, txRestriction, evaluationPeriodId,
                                                laggingPeriodCount,
                                                isScoring, builder, sqlUser);
    }

    @SuppressWarnings("unchecked")
    private SQLQuery translateHasEngaged(QueryFactory queryFactory,
                                         AttributeRepository repository,
                                         TransactionRestriction txRestriction,
                                         int evaluationPeriodId,
                                         int laggingPeriodCount,
                                         boolean isScoring,
                                         String sqlUser) {

        TimeFilter timeFilter = txRestriction.getTimeFilter();
        String productIdStr = txRestriction.getProductId();
        boolean returnPositive = !txRestriction.isNegate();

        SQLQueryFactory factory = queryFactory.getSQLQueryFactory(repository, sqlUser);
        String txTableName = getPeriodTransactionTableName(repository);
        StringPath tablePath = Expressions.stringPath(txTableName);
        String period = txRestriction.getTimeFilter().getPeriod();

        NumberExpression trxnAmountValNumber = Expressions.numberPath(BigDecimal.class, trxnAmountVal.getMetadata());
        NumberExpression trxnQuantityValNumber = Expressions.numberPath(BigDecimal.class, trxnQuantityVal.getMetadata());
        CaseBuilder caseBuilder = new CaseBuilder();
        NumberExpression trxnValExists = caseBuilder.when(trxnAmountValNumber.gt(0)).then(1)
            .when(trxnQuantityValNumber.gt(0)).then(1)
            .otherwise(0);

        Expression windowAgg = translateTimeWindow(timeFilter, SQLExpressions.max(trxnValExists).over()
            .partitionBy(keysAccountId).orderBy(keysPeriodId)).as(amountAggr);

        SQLQuery productQuery = factory.query()  //
            .select(accountId, periodId, amountVal.as(AMOUNT_VAL), quantityVal.as(QUANTITY_VAL)).from(tablePath) //
            .where(periodName.eq(period)) //
            .where(translateProductId(productIdStr));

        SQLQuery apsQuery = factory.query().select(keysAccountId, keysPeriodId, trxnAmountVal, windowAgg)
            .from(keysPath).leftJoin(productQuery, trxnPath)
            .on(keysAccountId.eq(trxnAccountId).and(keysPeriodId.eq(trxnPeriodId)));

        BooleanExpression periodIdPredicate = translatePeriodRestriction(queryFactory, repository, isScoring, period,
                                                                         evaluationPeriodId, laggingPeriodCount,
                                                                         sqlUser);

        int expectedResult = (returnPositive) ? 1 : 0;

        return factory.query().select(accountId, periodId).from(apsQuery, apsPath)
            .where(amountAggr.eq(String.valueOf(expectedResult)).and(periodIdPredicate))
            .groupBy(accountId, periodId);

    }


    private SubQuery translateSelectAll(QueryFactory queryFactory,
                                        AttributeRepository repository,
                                        String tableName, String sqlUser) {
        SQLQueryFactory factory = queryFactory.getSQLQueryFactory(repository, sqlUser);
        EntityPath<String> tablePath = new PathBuilder<>(String.class, tableName);
        SQLQuery selectAll = factory.query().select(SQLExpressions.all).from(tablePath);
        SubQuery query = new SubQuery();
        query.setSubQueryExpression(selectAll);
        query.setAlias(tableName);
        return query;
    }

    private SubQuery translateEventRevenue(QueryFactory queryFactory,
                                           AttributeRepository repository,
                                           String eventTupleViewAlias,
                                           String sqlUser) {
        SQLQuery selectAll = joinEventTupleWithRevenue(queryFactory, repository, eventTupleViewAlias, sqlUser);
        SubQuery query = new SubQuery();
        query.setSubQueryExpression(selectAll);
        query.setAlias(generateAlias("Revenue"));
        return query.withProjection(ACCOUNT_ID).withProjection(PERIOD_ID).withProjection(REVENUE);
    }

    private SubQuery translateTransactionRestriction(QueryFactory queryFactory,
                                                     AttributeRepository repository,
                                                     TransactionRestriction txRestriction,
                                                     int evaluationPeriodId,
                                                     int laggingPeriodCount,
                                                     boolean isScoring,
                                                     QueryBuilder builder,
                                                     String sqlUser) {
        if (StringUtils.isEmpty(txRestriction.getProductId())) {
            throw new RuntimeException("Invalid transaction restriction, no product specified");
        }
        SQLQuery subQueryExpression = translateTransaction(queryFactory, repository, txRestriction, evaluationPeriodId,
                                                           laggingPeriodCount,
                                                           isScoring, builder, sqlUser);
        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(subQueryExpression);
        subQuery.setAlias(generateAlias(BusinessEntity.Transaction.name()));
        return subQuery.withProjection(ACCOUNT_ID).withProjection(PERIOD_ID);
    }

    private SubQuery translateAccountViewWithJoinedPeriods(QueryFactory queryFactory,
                                                           AttributeRepository repository,
                                                           String accountViewAlias,
                                                           String period,
                                                           int evaluationPeriodId,
                                                           int laggingPeriodCount,
                                                           boolean isScoring,
                                                           String sqlUser) {
        SQLQuery subQueryExpression = joinAccountWithPeriods(queryFactory, repository, accountViewAlias, period,
                                                             evaluationPeriodId, laggingPeriodCount, isScoring, sqlUser);
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

        TimeFilter timeFilter = new TimeFilter(
            priorOnly.getTimeFilter().getLhs(), ComparisonType.PRIOR, //
            priorOnly.getTimeFilter().getPeriod(), priorOnly.getTimeFilter().getValues());
        return new TransactionRestriction(priorOnly.getProductId(), //
                                          timeFilter, //
                                          false, //
                                          priorOnly.getSpentFilter(), //
                                          priorOnly.getUnitFilter());
    }

    private TransactionRestriction translateToNotEngagedWithin(TransactionRestriction priorOnly) {

        TimeFilter timeFilter = new TimeFilter(
            priorOnly.getTimeFilter().getLhs(), ComparisonType.WITHIN, //
            priorOnly.getTimeFilter().getPeriod(), priorOnly.getTimeFilter().getValues());

        return new TransactionRestriction(priorOnly.getProductId(), //
                                          timeFilter, //
                                          true, //
                                          null, //
                                          null);
    }

    private TransactionRestriction translateToEngagedWithin(TransactionRestriction priorOnly) {

        TimeFilter timeFilter = new TimeFilter(
            priorOnly.getTimeFilter().getLhs(), ComparisonType.WITHIN, //
            priorOnly.getTimeFilter().getPeriod(), priorOnly.getTimeFilter().getValues());

        return new TransactionRestriction(priorOnly.getProductId(), //
                                          timeFilter, //
                                          false, //
                                          null, //
                                          null);
    }

    private TransactionRestriction translateToNotEngagedEver(TransactionRestriction priorOnly) {

        TimeFilter timeFilter = new TimeFilter(
            priorOnly.getTimeFilter().getLhs(), ComparisonType.EVER, //
            priorOnly.getTimeFilter().getPeriod(), priorOnly.getTimeFilter().getValues());

        return new TransactionRestriction(priorOnly.getProductId(), //
                                          timeFilter, //
                                          true, //
                                          null, //
                                          null);
    }

    private boolean calculateEventRevenue(boolean isEvent, EventFrontEndQuery frontEndQuery) {
        return isEvent && frontEndQuery.getCalculateProductRevenue();
    }

    private QueryBuilder translateRestriction(QueryFactory queryFactory,
                                              AttributeRepository repository,
                                              Restriction restriction,
                                              boolean isScoring,
                                              boolean isEvent,
                                              EventFrontEndQuery frontEndQuery,
                                              QueryBuilder builder,
                                              String sqlUser) {

        final String period = frontEndQuery.getPeriodName();
        final int periodCount = frontEndQuery.getPeriodCount();
        final int laggingPeriodCount = frontEndQuery.getLaggingPeriodCount();
        final int evaluationPeriodId = frontEndQuery.getEvaluationPeriodId();

        if (laggingPeriodCount < ONE_LAG_BEHIND_OFFSET) {
            throw new IllegalArgumentException("Invalid lagging period count: " + laggingPeriodCount);
        }

        if (StringUtils.isBlank(period)) {
            throw new IllegalArgumentException("Must specify a period name in front end query: " + JsonUtils.serialize(frontEndQuery));
        }

        Map<LogicalRestriction, List<String>> subQueryTableMap = new HashMap<>();
        Restriction rootRestriction = restriction;
        overwritePeriodsInRestriction(restriction, period);
        builder.with(translateAllKeys(queryFactory, repository, period, periodCount, sqlUser));

        if (calculateEventRevenue(isEvent, frontEndQuery)) {
            if (frontEndQuery.getTargetProductIds() == null || frontEndQuery.getTargetProductIds().isEmpty()) {
                throw new RuntimeException("Fail to calculate product revenue. No target product specified");
            }
            builder.with(translateProductRevenue(queryFactory, repository, frontEndQuery.getTargetProductIds(),
                                                 period, sqlUser));
            builder.with(translateShiftedRevenue(queryFactory, repository, laggingPeriodCount, sqlUser));
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

        // translate restrictions to individual subqueries
        if (rootRestriction instanceof LogicalRestriction) {
            BreadthFirstSearch bfs = new BreadthFirstSearch();
            bfs.run(rootRestriction, (object, ctx) -> {
                if (object instanceof LogicalRestriction) {
                    LogicalRestriction logicalRestriction = (LogicalRestriction) object;
                    subQueryTableMap.put(logicalRestriction, new ArrayList<>());
                } else if (object instanceof TransactionRestriction) {
                    TransactionRestriction txRestriction = (TransactionRestriction) object;
                    SubQuery subQuery = translateTransactionRestriction(queryFactory, repository, txRestriction,
                                                                        evaluationPeriodId,
                                                                        laggingPeriodCount,
                                                                        isScoring, builder, sqlUser);
                    builder.with(subQuery);
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    List<String> childSubQueryList = subQueryTableMap.get(parent);
                    childSubQueryList.add(subQuery.getAlias());
                } else if (object instanceof ConcreteRestriction) {
                    ConcreteRestriction concreteRestriction = (ConcreteRestriction) object;
                    SubQuery accountViewSubquery = translateAccountView(concreteRestriction);
                    builder.with(accountViewSubquery);
                    SubQuery subQuery = translateAccountViewWithJoinedPeriods(queryFactory, repository,
                                                                              accountViewSubquery.getAlias(),
                                                                              period, evaluationPeriodId,
                                                                              laggingPeriodCount,
                                                                              isScoring, sqlUser);
                    builder.with(subQuery);
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    List<String> childSubQueryList = subQueryTableMap.get(parent);
                    childSubQueryList.add(subQuery.getAlias());
                }
            });
        } else if (rootRestriction instanceof TransactionRestriction) {
            TransactionRestriction txRestriction = (TransactionRestriction) rootRestriction;
            SubQuery subQuery = translateTransactionRestriction(queryFactory, repository, txRestriction,
                                                                evaluationPeriodId,
                                                                laggingPeriodCount,
                                                                isScoring,
                                                                builder, sqlUser);
            builder.with(subQuery);
            SubQuery selectAll = translateSelectAll(queryFactory, repository, subQuery.getAlias(), sqlUser);
            SubQueryAttrLookup accountId = new SubQueryAttrLookup(selectAll, ACCOUNT_ID);
            SubQueryAttrLookup periodId = new SubQueryAttrLookup(selectAll, PERIOD_ID);
            builder.from(selectAll);
            builder.select(accountId, periodId);
        } else if (rootRestriction instanceof ConcreteRestriction) {
            ConcreteRestriction concreteRestriction = (ConcreteRestriction) rootRestriction;
            SubQuery accountViewSubquery = translateAccountView(concreteRestriction);
            builder.with(accountViewSubquery);
            SubQuery subQuery = translateAccountViewWithJoinedPeriods(queryFactory, repository,
                                                                      accountViewSubquery.getAlias(),
                                                                      period, evaluationPeriodId,
                                                                      laggingPeriodCount,
                                                                      isScoring, sqlUser);
            builder.with(subQuery);
            String selectAllAlias;
            if (calculateEventRevenue(isEvent, frontEndQuery)) {
                SubQuery revenueSubQuery = translateEventRevenue(queryFactory, repository, subQuery.getAlias(), sqlUser);
                builder.with(revenueSubQuery);
                selectAllAlias = revenueSubQuery.getAlias();
            } else {
                selectAllAlias = subQuery.getAlias();
            }
            SubQuery selectAll = translateSelectAll(queryFactory, repository, selectAllAlias, sqlUser);
            SubQueryAttrLookup accountId = new SubQueryAttrLookup(selectAll, ACCOUNT_ID);
            SubQueryAttrLookup periodId = new SubQueryAttrLookup(selectAll, PERIOD_ID);
            builder.from(selectAll);
            builder.select(accountId, periodId);
            if (calculateEventRevenue(isEvent, frontEndQuery)) {
                SubQueryAttrLookup revenue = new SubQueryAttrLookup(selectAll, REVENUE);
                builder.select(revenue);
            }
        } else {
            throw new UnsupportedOperationException("Cannot translate restriction " + restriction);
        }

        // merge subqueries into final query
        if (!subQueryTableMap.isEmpty()) {
            Map<LogicalRestriction, UnionCollector> unionMap = new HashMap<>();

            for (LogicalRestriction logicalRestriction : subQueryTableMap.keySet()) {
                List<String> childLookups = subQueryTableMap.get(logicalRestriction);
                // process leaf nodes
                if (childLookups != null && childLookups.size() != 0) {
                    SQLQuery[] childQueries = childLookups.stream().map(x -> {
                        return (SQLQuery) translateSelectAll(queryFactory, repository, x, sqlUser).getSubQueryExpression();
                    }).toArray(SQLQuery[]::new);

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
                    // todo: experiment with skipping intermediate tables to see if it improves performance
                    String mergedTableAlias = generateAlias("Logical");
                    SubQuery mergedQuery = new SubQuery();
                    mergedQuery.setSubQueryExpression(merged);
                    mergedQuery.setAlias(mergedTableAlias);
                    mergedQuery.withProjection(ACCOUNT_ID).withProjection(PERIOD_ID);
                    builder.with(mergedQuery);

                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    if (parent != null) {
                        SQLQuery childQuery = (SQLQuery)
                            translateSelectAll(queryFactory, repository, mergedTableAlias, sqlUser).getSubQueryExpression();

                        UnionCollector parentCollector = unionMap.getOrDefault(parent, new UnionCollector());
                        unionMap.put(parent, parentCollector.withUnion(childQuery));
                    } else {
                        String selectAllAlias;
                        if (calculateEventRevenue(isEvent, frontEndQuery)) {
                            SubQuery revenueSubQuery = translateEventRevenue(queryFactory, repository, mergedTableAlias, sqlUser);
                            builder.with(revenueSubQuery);
                            selectAllAlias = revenueSubQuery.getAlias();
                        } else {
                            selectAllAlias = mergedTableAlias;
                        }
                        SubQuery selectAll = translateSelectAll(queryFactory, repository, selectAllAlias, sqlUser);
                        SubQueryAttrLookup accountId = new SubQueryAttrLookup(selectAll, ACCOUNT_ID);
                        SubQueryAttrLookup periodId = new SubQueryAttrLookup(selectAll, PERIOD_ID);
                        builder.from(selectAll);
                        builder.select(accountId, periodId);
                        if (calculateEventRevenue(isEvent, frontEndQuery)) {
                            SubQueryAttrLookup revenue = new SubQueryAttrLookup(selectAll, REVENUE);
                            builder.select(revenue);
                        }
                    }
                }
            }, true);
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
            Restriction notEngagedEver = translateToNotEngagedEver(txRestriction);
            Restriction engagedWithin = translateToEngagedWithin(txRestriction);
            newRestriction = Restriction.builder().or(notEngagedEver, engagedWithin).build();
        }
        return newRestriction;
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
            return unionList.toArray(new SubQueryExpression[unionList.size()]);
        }
    }

    @SuppressWarnings({"unchecked", "rawtype"})
    private Union<?> mergeQueryResult(LogicalOperator ops, SubQueryExpression[] childQueries) {
        return (LogicalOperator.AND == ops) //
            ? SQLExpressions.intersect(childQueries)
            : SQLExpressions.union(childQueries);
    }
}
