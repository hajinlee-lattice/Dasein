package com.latticeengines.query.exposed.translator;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.ProductId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.TotalAmount;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.TotalQuantity;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.TransactionDate;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Transaction;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.RestrictionBuilder;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.latticeengines.query.util.AttrRepoUtils;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.sql.SQLQuery;

public class DateRangeTranslator extends TranslatorCommon {

    private static final BusinessEntity transaction = Transaction;
    private static final Set<ComparisonType> NEGATIVE_COMPARATORS = ImmutableSet.of( //
            ComparisonType.LESS_OR_EQUAL, //
            ComparisonType.LESS_THAN, //
            ComparisonType.IS_NULL);

    public Restriction convert(TransactionRestriction txnRestriction, QueryFactory queryFactory,
            AttributeRepository repository) {
        if (isHasNotPurchased(txnRestriction)) {
            SubQuery notPurchasedSubQuery = constructHasPurchasedSubQuery(txnRestriction, queryFactory, repository);
            return Restriction.builder() //
                    .let(BusinessEntity.Account, InterfaceName.AccountId.name()) //
                    .notInSubquery(notPurchasedSubQuery) //
                    .build();
        } else {
            SubQuery subQuery = constructSubQuery(txnRestriction, queryFactory, repository);
            RestrictionBuilder builder = Restriction.builder() //
                    .let(BusinessEntity.Account, InterfaceName.AccountId.name());
            if (Boolean.TRUE.equals(txnRestriction.isNegate())) {
                builder = builder.notInSubquery(subQuery);
            } else {
                builder = builder.inSubquery(subQuery);
            }
            return builder.build();
        }
    }

    private boolean isHasNotPurchased(TransactionRestriction txnRestriction) {
        AggregationFilter unitFilter = txnRestriction.getUnitFilter();
        AggregationFilter spendFilter = txnRestriction.getSpentFilter();
        return unitFilter == null && spendFilter == null && Boolean.TRUE.equals(txnRestriction.isNegate());
    }

    /**
     * Negative restriction means a restriction need to include null values
     */
    private boolean isNegativeRestriction(TransactionRestriction txnRestriction) {
        AggregationFilter unitFilter = txnRestriction.getUnitFilter();
        AggregationFilter spendFilter = txnRestriction.getSpentFilter();
        boolean unitIsNegative = isNegativeAggregation(unitFilter);
        boolean spendIsNegative = isNegativeAggregation(spendFilter);
        return (unitIsNegative && spendIsNegative) || (unitFilter == null && spendIsNegative)
                || (unitIsNegative && spendFilter == null);
    }

    private boolean isNegativeAggregation(AggregationFilter filter) {
        return filter != null && NEGATIVE_COMPARATORS.contains(filter.getComparisonType());
    }

    private SubQuery constructSubQuery(TransactionRestriction txnRestriction, QueryFactory queryFactory,
            AttributeRepository repository) {
        StringPath table = AttrRepoUtils.getTablePath(repository, transaction);
        BooleanExpression productPredicate = getProductPredicate(txnRestriction.getProductId());
        BooleanExpression datePredicate = getDatePredicate(txnRestriction.getTimeFilter());
        BooleanExpression predicate = productPredicate;
        if (datePredicate != null) {
            predicate = productPredicate.and(datePredicate);
        }
        SQLQuery<?> query = queryFactory.getQuery(repository) //
                .select(accountId) //
                .from(table) //
                .where(predicate);
        BooleanExpression aggPredicate = getAggPredicate(txnRestriction);
        if (aggPredicate != null) {
            query = query.groupBy(accountId).having(aggPredicate);
        }

        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(query);
        return subQuery;
    }

    private SubQuery constructHasPurchasedSubQuery(TransactionRestriction txnRestriction, QueryFactory queryFactory,
            AttributeRepository repository) {
        StringPath table = AttrRepoUtils.getTablePath(repository, transaction);
        BooleanExpression productPredicate = getProductPredicate(txnRestriction.getProductId());
        BooleanExpression datePredicate = getDatePredicate(txnRestriction.getTimeFilter());
        BooleanExpression predicate = productPredicate;
        if (datePredicate != null) {
            predicate = productPredicate.and(datePredicate);
        }
        SQLQuery<?> query = queryFactory.getQuery(repository) //
                .select(accountId) //
                .from(table) //
                .where(predicate);
        SubQuery subQuery = new SubQuery();
        subQuery.setSubQueryExpression(query);
        return subQuery;
    }

    private BooleanExpression getProductPredicate(String productId) {
        String[] productIds = productId.split(",");
        StringPath productIdPath = Expressions.stringPath(ProductId.name());
        if (productIds.length == 1) {
            return productIdPath.eq(productId);
        } else {
            return productIdPath.in(Arrays.asList(productIds));
        }
    }

    private BooleanExpression getDatePredicate(TimeFilter timeFilter) {
        if (ComparisonType.EVER.equals(timeFilter.getRelation())) {
            return null;
        }
        if (!TimeFilter.Period.Date.name().equals(timeFilter.getPeriod())) {
            throw new UnsupportedOperationException(
                    "Can only translate Date period, but " + timeFilter.getPeriod() + " was given.");
        }

        StringPath datePath = Expressions.stringPath(TransactionDate.name());

        List<Object> vals = timeFilter.getValues();
        BooleanExpression predicate1 = vals.get(0) == null ? null : datePath.goe((String) vals.get(0));
        BooleanExpression predicate2 = vals.get(1) == null ? null : datePath.loe((String) vals.get(1));
        return mergePredicates(predicate1, predicate2);
    }

    private BooleanExpression mergePredicates(BooleanExpression... predicates) {
        BooleanExpression merged = null;
        for (BooleanExpression predicate : predicates) {
            if (predicate != null) {
                if (merged == null) {
                    merged = predicate;
                } else {
                    merged = merged.and(predicate);
                }
            }
        }
        return merged;
    }

    private BooleanExpression getAggPredicate(TransactionRestriction txnRestriction) {
        AggregationFilter spentFilter = txnRestriction.getSpentFilter();
        AggregationFilter unitFilter = txnRestriction.getUnitFilter();

        StringPath amtAgg = Expressions.stringPath(TotalAmount.name());
        StringPath qtyAgg = Expressions.stringPath(TotalQuantity.name());

        BooleanExpression aggrAmountPredicate = (spentFilter != null)
                ? translateAggregatePredicate(amtAgg, spentFilter, true) : null;
        BooleanExpression aggrQuantityPredicate = (unitFilter != null)
                ? translateAggregatePredicate(qtyAgg, unitFilter, true) : null;

        BooleanExpression aggrValPredicate = mergePredicates(aggrAmountPredicate, aggrQuantityPredicate);

        if (aggrValPredicate != null && txnRestriction.isNegate()) {
            aggrValPredicate = aggrValPredicate.not();
        }

        return aggrValPredicate;
    }

}
