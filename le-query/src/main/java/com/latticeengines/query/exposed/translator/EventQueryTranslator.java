package com.latticeengines.query.exposed.translator;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;

import com.latticeengines.common.exposed.graph.traversal.impl.BreadthFirstSearch;
import com.latticeengines.common.exposed.graph.traversal.impl.DepthFirstSearch;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AggregationType;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.LogicalOperator;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.QueryBuilder;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.Sort;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.domain.exposed.util.RestrictionUtils;
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

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AggregatedTransaction;

public class EventQueryTranslator {
    public static final int NUM_ADDITIONAL_PERIOD = 2;
    public static final int ONE_LEG_BEHIND_OFFSET = 1;
    public static final String ACCOUNT_ID = InterfaceName.AccountId.name();
    public static final String PERIOD_ID = InterfaceName.PeriodId.name();
    public static final String PRODUCT_ID = InterfaceName.ProductId.name();
    public static final String TOTAL_AMOUNT = InterfaceName.TotalAmount.name();
    public static final String TOTAL_QUANTITY = InterfaceName.TotalQuantity.name();
    public static final String TXRN = "txrn";
    public static final String APS = "aps";
    public static final String AMOUNT_AGG = "amountagg";
    public static final String QUANTITY_AGG = "quantityagg";
    public static final String AMOUNT_VAL = "amountval";
    public static final String QUANTITY_VAL = "quantityval";
    public static final String MAX_PERIOD = "maxperiod";
    public static final String MAX_PERIOD_ID = "maxperiodid";
    public static final String KEYS = "keys";

    private StringPath accountId = Expressions.stringPath(ACCOUNT_ID);
    private StringPath periodId = Expressions.stringPath(PERIOD_ID);
    private StringPath productId = Expressions.stringPath(PRODUCT_ID);
    private StringPath amountVal = Expressions.stringPath(TOTAL_AMOUNT);
    private StringPath quantityVal = Expressions.stringPath(TOTAL_QUANTITY);
    private StringPath amountAggr = Expressions.stringPath(AMOUNT_AGG);
    private StringPath quantityAggr = Expressions.stringPath(QUANTITY_AGG);

    private EntityPath<String> keysPath = new PathBuilder<>(String.class, KEYS);
    private EntityPath<String> trxnPath = new PathBuilder<>(String.class, TXRN);
    private EntityPath<String> apsPath = new PathBuilder<>(String.class, APS);
    private StringPath keysAccountId = Expressions.stringPath(keysPath, ACCOUNT_ID);
    private StringPath trxnAccountId = Expressions.stringPath(trxnPath, ACCOUNT_ID);
    private StringPath keysPeriodId = Expressions.stringPath(keysPath, PERIOD_ID);
    private StringPath trxnPeriodId = Expressions.stringPath(trxnPath, PERIOD_ID);
    private StringPath trxnAmountVal = Expressions.stringPath(trxnPath, AMOUNT_VAL);
    private StringPath trxnQuantityVal = Expressions.stringPath(trxnPath, QUANTITY_VAL);
    private StringPath trxnVal = Expressions.stringPath(trxnPath, AMOUNT_VAL);

    public QueryBuilder translateForScoring(QueryFactory queryFactory,
                                            AttributeRepository repository,
                                            Restriction restriction,
                                            QueryBuilder queryBuilder) {
        return translateRestriction(queryFactory, repository, translateFrontendRestriction(restriction), true, false,
                                    queryBuilder);
    }

    public QueryBuilder translateForTraining(QueryFactory queryFactory,
                                             AttributeRepository repository,
                                             Restriction restriction,
                                             QueryBuilder queryBuilder) {
        return translateRestriction(queryFactory, repository, translateFrontendRestriction(restriction), false, false,
                                    queryBuilder);
    }

    public QueryBuilder translateForEvent(QueryFactory queryFactory,
                                          AttributeRepository repository,
                                          Restriction restriction,
                                          QueryBuilder queryBuilder) {
        return translateRestriction(queryFactory, repository, translateFrontendRestriction(restriction), false, true,
                                    queryBuilder);
    }


    private SQLQueryFactory getSQLQueryFactory(QueryFactory queryFactory, AttributeRepository repository) {
        return queryFactory.getSQLQueryFactory(repository);
    }

    protected String getPeriodTransactionTableName(AttributeRepository repository) {
        // todo, need to change this when new period transaction is defined
        return repository.getTableName(AggregatedTransaction);
    }

    // todo, need to change this when new period transaction is defined
    private BusinessEntity getPeriodTransaction() {
        return BusinessEntity.Transaction;
    }

    @SuppressWarnings({"unchecked", "rawtype"})
    private SubQuery translateAllKeys(QueryFactory queryFactory, AttributeRepository repository) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository);

        String txTableName = getPeriodTransactionTableName(repository);
        StringPath tablePath = Expressions.stringPath(txTableName);
        StringPath accountId = Expressions.stringPath("accountid");
        StringPath periodId = Expressions.stringPath("periodid");
        EntityPath<String> periodRange = new PathBuilder<>(String.class, "periodRange");
        NumberPath minPid = Expressions.numberPath(BigDecimal.class, periodRange, "minpid");
        StringPath periodAccountId = Expressions.stringPath(periodRange, "accountid");
        SQLQuery periodRangeSubQuery =
                factory.query().select(accountId, SQLExpressions.min(periodId).as("minpid")).from(tablePath).groupBy(accountId);

        EntityPath<String> crossProd = new PathBuilder<>(String.class, "crossprod");
        StringPath crossAccountId = Expressions.stringPath(crossProd, "accountid");
        StringPath crossPeriodId = Expressions.stringPath(crossProd, "periodid");
        SQLQuery crossProdQuery = factory.query().select(accountId, periodId).from(
                factory.selectDistinct(accountId).from(tablePath).as("allaccounts"),
                factory.selectDistinct(periodId).from(tablePath).as("allperiods"));

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
                                          AttributeRepository repository) {
        SQLQueryFactory factory = getSQLQueryFactory(queryFactory, repository);
        String txTableName = getPeriodTransactionTableName(repository);
        StringPath tablePath = Expressions.stringPath(txTableName);
        StringPath periodId = Expressions.stringPath(PERIOD_ID);

        SQLQuery maxPeriodIdSubQuery = factory.query().from(tablePath).select(periodId.max().as(MAX_PERIOD_ID));
        return maxPeriodIdSubQuery;
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
        Object value = aggregationFilter.getValues().get(0);

        BooleanExpression aggrPredicate = null;
        switch (aggregateType) {
        case SUM:
        case AVG:
            aggrPredicate = toBooleanExpression(aggr, cmp, value);
            break;
        case AT_LEAST_ONCE:
        case EACH:
            aggrPredicate = aggr.eq(String.valueOf(1));
            break;
        }

        return aggrPredicate;
    }

    private BooleanExpression toBooleanExpression(StringPath numberPath, ComparisonType cmp, Object value) {
        switch (cmp) {
        case GREATER_OR_EQUAL:
            return numberPath.goe(value.toString());
        case GREATER_THAN:
            return numberPath.gt(value.toString());
        case LESS_THAN:
            return numberPath.lt(value.toString());
        case LESS_OR_EQUAL:
            return numberPath.loe(value.toString());
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
                                                        boolean ascendindPeriod) {
        NumberExpression trxnValNumber = Expressions.numberPath(BigDecimal.class, trxnVal.getMetadata());


        AggregationType aggregateType = aggregationFilter.getAggregationType();
        ComparisonType cmp = aggregationFilter.getComparisonType();
        Object value = aggregationFilter.getValues().get(0);

        WindowFunction windowAgg = null;
        switch (aggregateType) {
        case SUM:
            windowAgg = SQLExpressions.sum(trxnValNumber.coalesce(BigDecimal.ZERO)).over();
            break;
        case AVG:
            windowAgg = SQLExpressions.avg(trxnValNumber.coalesce(BigDecimal.ZERO)).over();
            break;
        case AT_LEAST_ONCE:
            BooleanExpression condition = toBooleanExpression(trxnVal, cmp, value);
            NumberExpression trxnValExists = new CaseBuilder().when(condition).then(1).otherwise(0);
            windowAgg = SQLExpressions.max(trxnValExists).over();
            break;
        case EACH:
            BooleanExpression each = toBooleanExpression(trxnVal, cmp, value);
            NumberExpression trxnValExistsOrNull = new CaseBuilder().when(each).then(1).otherwise(0);
            windowAgg = SQLExpressions.min(trxnValExistsOrNull).over();
            break;
        }

        if (ascendindPeriod) {
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
    private BooleanExpression translatePeriodRestriction(QueryFactory queryFactory,
                                                         AttributeRepository repository,
                                                         boolean isScoring,
                                                         StringPath periodId) {
        return (isScoring)
                ? periodId.eq(translateMaxPeriodId(queryFactory, repository))
                : periodId.lt(translateMaxPeriodId(queryFactory, repository));
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

        List<Expression> productSelectList = new ArrayList<>();
        productSelectList.addAll(Arrays.asList(accountId, periodId));
        List<Expression> apsSelectList = new ArrayList<>();
        apsSelectList.addAll(Arrays.asList(keysAccountId, keysPeriodId));

        if (spentFilter != null) {
            productSelectList.add(amountVal.as(AMOUNT_VAL));
            Expression spentWindowAgg = translateAggregateTimeWindow(keysAccountId, keysPeriodId, trxnAmountVal,
                                                                     timeFilter, spentFilter, true).as(amountAggr);
            apsSelectList.add(trxnAmountVal);
            apsSelectList.add(spentWindowAgg);
        }

        if (unitFilter != null) {
            productSelectList.add(quantityVal.as(QUANTITY_VAL));
            Expression unitWindowAgg =
                    translateAggregateTimeWindow(keysAccountId, keysPeriodId, trxnQuantityVal, timeFilter, unitFilter, true)
                            .as(quantityAggr);
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
        BooleanExpression periodIdPredicate = translatePeriodRestriction(queryFactory, repository, isScoring, periodId);

        SQLQuery finalQuery = factory.query().select(accountId, periodId).from(apsQuery, apsPath)
                .where(aggrValPredicate.and(periodIdPredicate));

        return finalQuery;

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

        NumberExpression trxnValNumber = Expressions.numberPath(BigDecimal.class, trxnVal.getMetadata());
        CaseBuilder caseBuilder = new CaseBuilder();
        NumberExpression trxnValExists = caseBuilder.when(trxnValNumber.goe(0)).then(1).otherwise(0);

        Expression windowAgg = translateTimeWindow(timeFilter, SQLExpressions.max(trxnValExists).over()
                .partitionBy(keysAccountId).orderBy(keysPeriodId)).as(amountAggr);

        SQLQuery productQuery = factory.query().select(accountId, periodId, amountVal.as(AMOUNT_VAL)).from(tablePath)
                .where(translateProductId(productIdStr));

        SQLQuery apsQuery = factory.query().select(keysAccountId, keysPeriodId, trxnVal, windowAgg)
                .from(keysPath).leftJoin(productQuery, trxnPath)
                .on(keysAccountId.eq(trxnAccountId).and(keysPeriodId.eq(trxnPeriodId)));

        BooleanExpression periodIdPredicate = translatePeriodRestriction(queryFactory, repository, isScoring, periodId);

        int expectedResult = (returnPositive) ? 1 : 0;

        SQLQuery finalQuery = factory.query().select(accountId, periodId).from(apsQuery, apsPath)
                .where(amountAggr.eq(String.valueOf(expectedResult)).and(periodIdPredicate));

        return finalQuery;

    }

    private TransactionRestriction translateOneLegBehindRestriction(TransactionRestriction original) {
        TimeFilter timeFilter = new TimeFilter(original.getTimeFilter().getLhs(),
                                               ComparisonType.FOLLOWING,
                                               original.getTimeFilter().getPeriod(),
                                               Collections.singletonList(ONE_LEG_BEHIND_OFFSET));

        TransactionRestriction oneLegBehind = new TransactionRestriction(original.getProductName(), //
                                                                         original.getProductId(), //
                                                                         timeFilter, //
                                                                         false, //
                                                                         null, //
                                                                         null);
        return oneLegBehind;
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

    private String generateAlias(String prefix) {
        return prefix + RandomStringUtils.randomAlphanumeric(8);
    }

    private SubQuery translateTransactionRestriction(QueryFactory queryFactory,
                                                     AttributeRepository repository,
                                                     TransactionRestriction txRestriction,
                                                     boolean isScoring) {
        SQLQuery subQueryExpression = translateTransaction(queryFactory, repository, txRestriction, isScoring);
        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(subQueryExpression);
        subQuery.setAlias(generateAlias(BusinessEntity.Transaction.name()));
        return subQuery.withProjection(ACCOUNT_ID).withProjection(PERIOD_ID);
    }

    private SubQuery translateConcreteRestriction(QueryFactory queryFactory,
                                                  AttributeRepository repository,
                                                  ConcreteRestriction restriction,
                                                  boolean isScoring) {
        BusinessEntity txAggregation = getPeriodTransaction();

        AttributeLookup accountId = new AttributeLookup(BusinessEntity.Account, ACCOUNT_ID);
        AttributeLookup txAccountId = new AttributeLookup(txAggregation, ACCOUNT_ID);
        AttributeLookup txPeriodId = new AttributeLookup(txAggregation, PERIOD_ID);


        Query accountQuery = Query.builder().select(accountId).from(BusinessEntity.Account).where(restriction).build();
        SubQuery inAccountSubQuery = new SubQuery(accountQuery, generateAlias(BusinessEntity.Account.name()));
        ConcreteRestriction accountInRestriction = (ConcreteRestriction) Restriction.builder()
                .let(txAggregation, InterfaceName.AccountId.name())
                .inCollection(inAccountSubQuery, ACCOUNT_ID).build();

        SubQuery periodIdSubQuery = new SubQuery();
        periodIdSubQuery.setSubQueryExpression(translateMaxPeriodId(queryFactory, repository));
        periodIdSubQuery.setAlias(MAX_PERIOD);
        SubQueryAttrLookup maxPeriodId = new SubQueryAttrLookup(periodIdSubQuery, MAX_PERIOD_ID);

        // target or training
        Restriction periodIdRestriction = null;
        if (isScoring) {
            Restriction.builder().let(txPeriodId).eq(maxPeriodId).build();
        } else {
            periodIdRestriction = Restriction.builder().let(txPeriodId).not().eq(maxPeriodId).build();
        }

        Restriction accountInPeriod = Restriction.builder().and(accountInRestriction, periodIdRestriction).build();
        Query txQuery = Query.builder().select(txAccountId, txPeriodId)
                .from(txAggregation).where(accountInPeriod).build();
        SubQuery subQuery = new SubQuery(txQuery, generateAlias(BusinessEntity.Account.name()));
        return subQuery.withProjection(ACCOUNT_ID).withProjection(PERIOD_ID);
    }

    private TransactionRestriction translateToPrior(TransactionRestriction priorOnly) {

        TimeFilter timeFilter = new TimeFilter(
                priorOnly.getTimeFilter().getLhs(), ComparisonType.PRIOR, //
                priorOnly.getTimeFilter().getPeriod(), priorOnly.getTimeFilter().getValues());
        TransactionRestriction prior = new TransactionRestriction(priorOnly.getProductName(), //
                                                                  priorOnly.getProductId(), //
                                                                  timeFilter, //
                                                                  false, //
                                                                  priorOnly.getSpentFilter(), //
                                                                  priorOnly.getUnitFilter());
        return prior;
    }

    private TransactionRestriction translateToNotEngagedWithin(TransactionRestriction priorOnly) {

        TimeFilter timeFilter = new TimeFilter(
                priorOnly.getTimeFilter().getLhs(), ComparisonType.WITHIN, //
                priorOnly.getTimeFilter().getPeriod(), priorOnly.getTimeFilter().getValues());

        TransactionRestriction notWithin = new TransactionRestriction(priorOnly.getProductName(), //
                                                                      priorOnly.getProductId(), //
                                                                      timeFilter, //
                                                                      true, //
                                                                      null, //
                                                                      null);
        return notWithin;
    }

    private TransactionRestriction translateToEngagedWithin(TransactionRestriction priorOnly) {

        TimeFilter timeFilter = new TimeFilter(
                priorOnly.getTimeFilter().getLhs(), ComparisonType.WITHIN, //
                priorOnly.getTimeFilter().getPeriod(), priorOnly.getTimeFilter().getValues());

        TransactionRestriction engagedWithin = new TransactionRestriction(priorOnly.getProductName(), //
                                                                          priorOnly.getProductId(), //
                                                                          timeFilter, //
                                                                          false, //
                                                                          null, //
                                                                          null);
        return engagedWithin;
    }

    private TransactionRestriction translateToNotEngagedEver(TransactionRestriction priorOnly) {

        TimeFilter timeFilter = new TimeFilter(
                priorOnly.getTimeFilter().getLhs(), ComparisonType.EVER, //
                priorOnly.getTimeFilter().getPeriod(), priorOnly.getTimeFilter().getValues());

        TransactionRestriction notEver = new TransactionRestriction(priorOnly.getProductName(), //
                                                                    priorOnly.getProductId(), //
                                                                    timeFilter, //
                                                                    true, //
                                                                    null, //
                                                                    null);
        return notEver;
    }

    private QueryBuilder translateRestriction(QueryFactory queryFactory,
                                              AttributeRepository repository,
                                              Restriction restriction,
                                              boolean isScoring,
                                              boolean checkNextPeriod,
                                              QueryBuilder builder) {

        builder.with(translateAllKeys(queryFactory, repository));

        Map<LogicalRestriction, List<String>> subQueryTableMap = new HashMap<>();
        Restriction rootRestriction = restriction;
        Sort sort = new Sort();

        // combine one leg behind restriction for event query, this is not needed for scoring and training
        if (!isScoring && checkNextPeriod) {
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
                    SubQuery subQuery = translateTransactionRestriction(queryFactory, repository, txRestriction, isScoring);
                    builder.with(subQuery);
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    List<String> childSubQueryList = subQueryTableMap.get(parent);
                    childSubQueryList.add(subQuery.getAlias());
                } else if (object instanceof ConcreteRestriction) {
                    ConcreteRestriction concreteRestriction = (ConcreteRestriction) object;
                    SubQuery subQuery = translateConcreteRestriction(queryFactory, repository, concreteRestriction, isScoring);
                    builder.with(subQuery);
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    List<String> childSubQueryList = subQueryTableMap.get(parent);
                    childSubQueryList.add(subQuery.getAlias());
                }
            });
        } else if (rootRestriction instanceof TransactionRestriction) {
            TransactionRestriction txRestriction = (TransactionRestriction) rootRestriction;
            SubQuery subQuery = translateTransactionRestriction(queryFactory, repository, txRestriction, isScoring);
            builder.with(subQuery);
            SubQuery selectAll = translateSelectAll(queryFactory, repository, subQuery.getAlias());
            SubQueryAttrLookup accountId = new SubQueryAttrLookup(selectAll, ACCOUNT_ID);
            SubQueryAttrLookup periodId = new SubQueryAttrLookup(selectAll, PERIOD_ID);
            builder.from(selectAll);
            builder.select(accountId, periodId);
            sort.setLookups(Arrays.asList(accountId, periodId));
        } else if (rootRestriction instanceof ConcreteRestriction) {
            ConcreteRestriction concreteRestriction = (ConcreteRestriction) rootRestriction;
            SubQuery subQuery = translateConcreteRestriction(queryFactory, repository, concreteRestriction, isScoring);
            builder.with(subQuery);
            SubQuery selectAll = translateSelectAll(queryFactory, repository, subQuery.getAlias());
            SubQueryAttrLookup accountId = new SubQueryAttrLookup(selectAll, ACCOUNT_ID);
            SubQueryAttrLookup periodId = new SubQueryAttrLookup(selectAll, PERIOD_ID);
            builder.from(selectAll);
            builder.select(accountId, periodId);
            sort.setLookups(Arrays.asList(accountId, periodId));
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
                        sort.setLookups(Arrays.asList(accountId, periodId));
                    }
                }
            }, true);
        }

        return builder.orderBy(sort);
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

    // translate BucketRestriction
    private Restriction translateFrontendRestriction(Restriction restriction) {
        Restriction translated;
        if (restriction instanceof LogicalRestriction) {
            BreadthFirstSearch search = new BreadthFirstSearch();
            search.run(restriction, (object, ctx) -> {
                if (object instanceof BucketRestriction) {
                    BucketRestriction bucket = (BucketRestriction) object;
                    Restriction converted = RestrictionUtils.convertBucketRestriction(bucket);
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    parent.getRestrictions().remove(bucket);
                    parent.getRestrictions().add(converted);
                }
            });
            translated = restriction;
        } else if (restriction instanceof BucketRestriction) {
            BucketRestriction bucket = (BucketRestriction) restriction;
            translated = RestrictionUtils.convertBucketRestriction(bucket);
        } else {
            translated = restriction;
        }

        return translated;
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
