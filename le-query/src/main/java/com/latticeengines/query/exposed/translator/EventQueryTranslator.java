package com.latticeengines.query.exposed.translator;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;

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
import com.latticeengines.common.exposed.graph.traversal.impl.BreadthFirstSearch;
import com.latticeengines.common.exposed.graph.traversal.impl.DepthFirstSearch;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
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
import com.latticeengines.query.exposed.factory.QueryFactory;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AggregatedPeriodTransaction;
import static com.latticeengines.query.exposed.translator.TranslatorUtils.generateAlias;

public class EventQueryTranslator extends TranslatorCommon {
    private static final int ONE_LEG_BEHIND_OFFSET = 1;

    public QueryBuilder translateForScoring(QueryFactory queryFactory,
                                            AttributeRepository repository,
                                            Restriction restriction,
                                            String periodName,
                                            QueryBuilder queryBuilder) {
        return translateRestriction(queryFactory, repository, restriction, true, false,
                                    periodName, queryBuilder);
    }

    public QueryBuilder translateForTraining(QueryFactory queryFactory,
                                             AttributeRepository repository,
                                             Restriction restriction,
                                             String periodName,
                                             QueryBuilder queryBuilder) {
        return translateRestriction(queryFactory, repository, restriction, false, false,
                                    periodName, queryBuilder);
    }

    public QueryBuilder translateForEvent(QueryFactory queryFactory,
                                          AttributeRepository repository,
                                          Restriction restriction,
                                          String periodName,
                                          QueryBuilder queryBuilder) {
        return translateRestriction(queryFactory, repository, restriction, false, true,
                                    periodName, queryBuilder);
    }


    private SQLQueryFactory getSQLQueryFactory(QueryFactory queryFactory, AttributeRepository repository) {
        return queryFactory.getSQLQueryFactory(repository);
    }

    protected String getPeriodTransactionTableName(AttributeRepository repository) {
        return repository.getTableName(AggregatedPeriodTransaction);
    }

    @SuppressWarnings({"unchecked", "rawtype"})
    private SubQuery translateAllKeys(QueryFactory queryFactory, AttributeRepository repository, String period) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository);

        String txTableName = getPeriodTransactionTableName(repository);
        StringPath tablePath = Expressions.stringPath(txTableName);
        NumberPath minPid = Expressions.numberPath(BigDecimal.class, periodRange, MIN_PID);
        SQLQuery periodRangeSubQuery = factory.query() //
                .select(accountId, SQLExpressions.min(periodId).as(MIN_PID)) //
                .from(tablePath) //
                .where(periodName.eq(period)) //
                .groupBy(accountId);

        SQLQuery crossProdQuery = factory.query().select(accountId, periodId).from(
                factory.selectDistinct(accountId).from(tablePath).where(periodName.eq(period)).as(ALL_ACCOUNTS),
                factory.selectDistinct(periodId).from(tablePath).where(periodName.eq(period)).as(ALL_PERIODS));

        SQLQuery crossProdSubQuery = factory.query().from(crossProdQuery, crossProd)
                .innerJoin(periodRangeSubQuery, periodRange).on(periodAccountId.eq(crossAccountId))
                .where(crossPeriodId.goe(minPid.subtract(NUM_ADDITIONAL_PERIOD)));
        SQLQuery minusProdSubQuery = crossProdSubQuery.select(crossAccountId, periodId);

        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(minusProdSubQuery);
        subQuery.setAlias(KEYS);
        return subQuery.withProjection(ACCOUNT_ID).withProjection(PERIOD_ID);
    }

    private SQLQuery translateMaxPeriodId(QueryFactory queryFactory,
                                          AttributeRepository repository,
                                          String period) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository);
        String txTableName = getPeriodTransactionTableName(repository);
        StringPath tablePath = Expressions.stringPath(txTableName);
        StringPath periodId = Expressions.stringPath(PERIOD_ID);

        return factory.query().from(tablePath) //
                .where(periodName.eq(period)) //
                .select(periodId.max().as(MAX_PID));
    }

    private BooleanExpression translateProductId(String productIdStr) {
        return productId.in(productIdStr.split(","));
    }

    @SuppressWarnings("unchecked")
    private BooleanExpression translatePeriodRestriction(QueryFactory queryFactory,
                                                         AttributeRepository repository,
                                                         boolean isScoring,
                                                         StringPath periodId,
                                                         String periodName) {
        return (isScoring)
                ? periodId.eq(translateMaxPeriodId(queryFactory, repository, periodName))
                : periodId.lt(translateMaxPeriodId(queryFactory, repository, periodName));
    }

    @SuppressWarnings("unchecked")
    private SQLQuery joinAccountWithPeriods(QueryFactory queryFactory,
                                            AttributeRepository repository,
                                            String accountViewAlias,
                                            String periodName,
                                            boolean isScoring) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository);

        EntityPath<String> accountViewPath = new PathBuilder<>(String.class, accountViewAlias);
        StringPath qualifiedAccountId = Expressions.stringPath(accountViewPath, ACCOUNT_ID);
        BooleanExpression periodIdPredicate = translatePeriodRestriction(queryFactory, repository, isScoring, periodId,
                periodName);
        return factory.query().select(keysAccountId, keysPeriodId)
                .from(keysPath)
                .join(accountViewPath)
                .on(keysAccountId.eq(qualifiedAccountId))
                .where(periodIdPredicate);
    }

    @SuppressWarnings("unchecked")
    private SQLQuery translateTransaction(QueryFactory queryFactory,
                                          AttributeRepository repository,
                                          TransactionRestriction txRestriction,
                                          boolean isScoring) {

        TimeFilter timeFilter = txRestriction.getTimeFilter();

        if (txRestriction.getSpentFilter() == null && txRestriction.getUnitFilter() == null) {
            return translateHasEngaged(queryFactory, repository, txRestriction, isScoring);
        }

        AggregationFilter spentFilter = txRestriction.getSpentFilter();
        AggregationFilter unitFilter = txRestriction.getUnitFilter();

        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository);

        String txTableName = getPeriodTransactionTableName(repository);
        StringPath tablePath = Expressions.stringPath(txTableName);

        List<Expression> productSelectList = new ArrayList(Arrays.asList(accountId, periodId));
        List<Expression> apsSelectList = new ArrayList(Arrays.asList(keysAccountId, keysPeriodId));

        productSelectList.add(amountVal.as(AMOUNT_VAL));
        apsSelectList.add(trxnAmountVal);

        if (spentFilter != null) {
            Expression spentWindowAgg = translateAggregateTimeWindow(keysAccountId, keysPeriodId, trxnAmountVal,
                                                                     timeFilter, spentFilter, true).as(amountAggr);
            apsSelectList.add(spentWindowAgg);
        }

        if (unitFilter != null) {
            productSelectList.add(quantityVal.as(QUANTITY_VAL));
            Expression unitWindowAgg = translateAggregateTimeWindow(keysAccountId, keysPeriodId, trxnQuantityVal,
                                                                    timeFilter, unitFilter, true).as(quantityAggr);
            apsSelectList.add(unitWindowAgg);
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

        BooleanExpression aggrAmountPredicate =
                (spentFilter != null) ? translateAggregatePredicate(amountAggr, spentFilter) : Expressions.TRUE;
        BooleanExpression aggrQuantityPredicate =
                (unitFilter != null) ? translateAggregatePredicate(quantityAggr, unitFilter) : Expressions.TRUE;

        BooleanExpression aggrValPredicate = aggrAmountPredicate.and(aggrQuantityPredicate);

        if (txRestriction.isNegate()) {
            aggrValPredicate = aggrValPredicate.not();
        }

        BooleanExpression periodIdPredicate = translatePeriodRestriction(queryFactory, repository, isScoring, periodId,
                                                                         period);

        return factory.query().select(accountId, periodId).from(apsQuery, apsPath)
                .where(aggrValPredicate.and(periodIdPredicate));

    }

    @SuppressWarnings("unchecked")
    private SQLQuery translateHasEngaged(QueryFactory queryFactory,
                                         AttributeRepository repository,
                                         TransactionRestriction txRestriction,
                                         boolean isScoring) {

        TimeFilter timeFilter = txRestriction.getTimeFilter();
        String productIdStr = txRestriction.getProductId();
        boolean returnPositive = !txRestriction.isNegate();

        SQLQueryFactory factory = queryFactory.getSQLQueryFactory(repository);
        String txTableName = getPeriodTransactionTableName(repository);
        StringPath tablePath = Expressions.stringPath(txTableName);
        String period = txRestriction.getTimeFilter().getPeriod();

        NumberExpression trxnValNumber = Expressions.numberPath(BigDecimal.class, trxnAmountVal.getMetadata());
        CaseBuilder caseBuilder = new CaseBuilder();
        NumberExpression trxnValExists = caseBuilder.when(trxnValNumber.goe(0)).then(1).otherwise(0);

        Expression windowAgg = translateTimeWindow(timeFilter, SQLExpressions.max(trxnValExists).over()
                .partitionBy(keysAccountId).orderBy(keysPeriodId)).as(amountAggr);

        SQLQuery productQuery = factory.query().select(accountId, periodId, amountVal.as(AMOUNT_VAL)).from(tablePath)
                .where(periodName.eq(period)) //
                .where(translateProductId(productIdStr));

        SQLQuery apsQuery = factory.query().select(keysAccountId, keysPeriodId, trxnAmountVal, windowAgg)
                .from(keysPath).leftJoin(productQuery, trxnPath)
                .on(keysAccountId.eq(trxnAccountId).and(keysPeriodId.eq(trxnPeriodId)));

        BooleanExpression periodIdPredicate = translatePeriodRestriction(queryFactory, repository, isScoring, periodId,
                                                                         period);

        int expectedResult = (returnPositive) ? 1 : 0;

        return factory.query().select(accountId, periodId).from(apsQuery, apsPath)
                .where(amountAggr.eq(String.valueOf(expectedResult)).and(periodIdPredicate));

    }

    private TransactionRestriction translateOneLegBehindRestriction(TransactionRestriction original) {
        TimeFilter timeFilter = new TimeFilter(original.getTimeFilter().getLhs(),
                                               ComparisonType.FOLLOWING,
                                               original.getTimeFilter().getPeriod(),
                                               Collections.singletonList(ONE_LEG_BEHIND_OFFSET));

        String targetProductId = original.getTargetProductId() == null
                ? original.getProductId()
                : original.getTargetProductId();

        if (StringUtils.isEmpty(targetProductId)) {
            throw new RuntimeException("Invalid transaction restriction, no target product specified");
        }

        return new TransactionRestriction(targetProductId, //
                                          timeFilter, //
                                          false, //
                                          null, //
                                          null);
    }


    private SubQuery translateSelectAll(QueryFactory queryFactory,
                                        AttributeRepository repository,
                                        String tableName) {
        SQLQueryFactory factory = queryFactory.getSQLQueryFactory(repository);
        EntityPath<String> tablePath = new PathBuilder<>(String.class, tableName);
        SQLQuery selectAll = factory.query().select(SQLExpressions.all).from(tablePath);
        SubQuery query = new SubQuery();
        query.setSubQueryExpression(selectAll);
        query.setAlias(tableName);
        return query;
    }

    private SubQuery translateTransactionRestriction(QueryFactory queryFactory,
                                                     AttributeRepository repository,
                                                     TransactionRestriction txRestriction,
                                                     boolean isScoring) {
        if (StringUtils.isEmpty(txRestriction.getProductId())) {
            throw new RuntimeException("Invalid transaction restriction, no product specified");
        }
        SQLQuery subQueryExpression = translateTransaction(queryFactory, repository, txRestriction, isScoring);
        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(subQueryExpression);
        subQuery.setAlias(generateAlias(BusinessEntity.Transaction.name()));
        return subQuery.withProjection(ACCOUNT_ID).withProjection(PERIOD_ID);
    }

    private SubQuery translateAccountViewWithJoinedPeriods(QueryFactory queryFactory,
                                                           AttributeRepository repository,
                                                           String accountViewAlias,
                                                           String period,
                                                           boolean isScoring) {
        SQLQuery subQueryExpression = joinAccountWithPeriods(queryFactory, repository, accountViewAlias, period, isScoring);
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

    private QueryBuilder translateRestriction(QueryFactory queryFactory,
                                              AttributeRepository repository,
                                              Restriction restriction,
                                              boolean isScoring,
                                              boolean isEvent,
                                              String periodName,
                                              QueryBuilder builder) {

        Map<LogicalRestriction, List<String>> subQueryTableMap = new HashMap<>();
        Restriction rootRestriction = restriction;

        String period = StringUtils.isEmpty(periodName) ? getPeriodFromRestriction(rootRestriction) : periodName;

        if (StringUtils.isEmpty(period)) {
            throw new RuntimeException("No period definition passed for event query.");
        }

        builder.with(translateAllKeys(queryFactory, repository, period));

        // combine one leg behind restriction for event query, this is not needed for scoring and training
        if (isEvent) {
            if (rootRestriction instanceof LogicalRestriction) {
                BreadthFirstSearch bfs = new BreadthFirstSearch();
                bfs.run(rootRestriction, (object, ctx) -> {
                    if (object instanceof TransactionRestriction) {
                        TransactionRestriction txRestriction = (TransactionRestriction) object;
                        TransactionRestriction oneLegBehind = translateOneLegBehindRestriction(txRestriction);
                        Restriction newRestriction = Restriction.builder().and(txRestriction, oneLegBehind).build();
                        LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                        parent.getRestrictions().remove(txRestriction);
                        parent.getRestrictions().add(newRestriction);
                    }
                });
            } else if (rootRestriction instanceof TransactionRestriction) {
                TransactionRestriction txRestriction = (TransactionRestriction) rootRestriction;
                TransactionRestriction oneLegBehind = translateOneLegBehindRestriction(txRestriction);
                rootRestriction = Restriction.builder().and(txRestriction, oneLegBehind).build();
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
                                                                        isScoring);
                    builder.with(subQuery);
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    List<String> childSubQueryList = subQueryTableMap.get(parent);
                    childSubQueryList.add(subQuery.getAlias());
                } else if (object instanceof ConcreteRestriction) {
                    ConcreteRestriction concreteRestriction = (ConcreteRestriction) object;
                    SubQuery accountViewSubquery = translateAccountView(concreteRestriction);
                    builder.with(accountViewSubquery);
                    SubQuery subQuery = translateAccountViewWithJoinedPeriods(queryFactory, repository,
                                                                              accountViewSubquery.getAlias(), period, isScoring);
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    List<String> childSubQueryList = subQueryTableMap.get(parent);
                    childSubQueryList.add(subQuery.getAlias());
                }
            });
        } else if (rootRestriction instanceof TransactionRestriction) {
            TransactionRestriction txRestriction = (TransactionRestriction) rootRestriction;
            SubQuery subQuery = translateTransactionRestriction(queryFactory, repository, txRestriction, isScoring
            );
            builder.with(subQuery);
            SubQuery selectAll = translateSelectAll(queryFactory, repository, subQuery.getAlias());
            SubQueryAttrLookup accountId = new SubQueryAttrLookup(selectAll, ACCOUNT_ID);
            SubQueryAttrLookup periodId = new SubQueryAttrLookup(selectAll, PERIOD_ID);
            builder.from(selectAll);
            builder.select(accountId, periodId);
        } else if (rootRestriction instanceof ConcreteRestriction) {
            ConcreteRestriction concreteRestriction = (ConcreteRestriction) rootRestriction;
            SubQuery accountViewSubquery = translateAccountView(concreteRestriction);
            builder.with(accountViewSubquery);
            SubQuery subQuery = translateAccountViewWithJoinedPeriods(queryFactory, repository,
                                                                      accountViewSubquery.getAlias(), period, isScoring);
            builder.with(subQuery);
            SubQuery selectAll = translateSelectAll(queryFactory, repository, subQuery.getAlias());
            SubQueryAttrLookup accountId = new SubQueryAttrLookup(selectAll, ACCOUNT_ID);
            SubQueryAttrLookup periodId = new SubQueryAttrLookup(selectAll, PERIOD_ID);
            builder.from(selectAll);
            builder.select(accountId, periodId);
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
                        return (SQLQuery) translateSelectAll(queryFactory, repository, x).getSubQueryExpression();
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
                                translateSelectAll(queryFactory, repository, mergedTableAlias).getSubQueryExpression();

                        UnionCollector parentCollector = unionMap.getOrDefault(parent, new UnionCollector());
                        unionMap.put(parent, parentCollector.withUnion(childQuery));
                    } else {
                        SubQuery selectAll = translateSelectAll(queryFactory, repository, mergedTableAlias);
                        SubQueryAttrLookup accountId = new SubQueryAttrLookup(selectAll, ACCOUNT_ID);
                        SubQueryAttrLookup periodId = new SubQueryAttrLookup(selectAll, PERIOD_ID);
                        builder.from(selectAll);
                        builder.select(accountId, periodId);
                    }
                }
            }, true);
        }
        return builder;
    }

    private String getPeriodFromRestriction(Restriction rootRestriction) {
        AtomicReference<String> periodRef = new AtomicReference<>(TimeFilter.Period.Month.name());

        // set transaction entity
        if (rootRestriction instanceof LogicalRestriction) {
            BreadthFirstSearch bfs = new BreadthFirstSearch();
            bfs.run(rootRestriction, (object, ctx) -> {
                if (object instanceof TransactionRestriction) {
                    TransactionRestriction txRestriction = (TransactionRestriction) object;
                    updatePeriodName(txRestriction, periodRef);
                }
            });
        } else if (rootRestriction instanceof TransactionRestriction) {
            TransactionRestriction txRestriction = (TransactionRestriction) rootRestriction;
            updatePeriodName(txRestriction, periodRef);
        }

        return periodRef.get();
    }

    private synchronized void updatePeriodName(TransactionRestriction txRestriction, AtomicReference<String> periodRef) {
        String periodInTxn = txRestriction.getTimeFilter().getPeriod();
        if (StringUtils.isBlank(periodRef.get())) {
            periodRef.set(periodInTxn);
        } else if (!periodInTxn.equals(periodRef.get())) {
            throw new LedpException(LedpCode.LEDP_37016, new String[]{ periodRef.get(), periodInTxn});
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
