package com.latticeengines.query.exposed.translator;

import java.util.Arrays;
import java.util.List;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
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
import com.querydsl.core.types.dsl.NumberPath;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.sql.SQLExpressions;
import com.querydsl.sql.SQLQuery;

public class DateRangeTranslator extends TranslatorCommon {

    private static final BusinessEntity transaction = BusinessEntity.Transaction;

    public static Restriction convert(TransactionRestriction txnRestriction, QueryFactory queryFactory,
            AttributeRepository repository, String sqlUser) {
        SubQuery subQuery = constructSubQuery(txnRestriction, queryFactory, repository, sqlUser, //
                Boolean.TRUE.equals(txnRestriction.isNegate()));
        RestrictionBuilder builder = Restriction.builder() //
                .let(BusinessEntity.Account, InterfaceName.AccountId.name())
                .inSubquery(subQuery);
        return builder.build();
    }

    private static SubQuery constructSubQuery(TransactionRestriction txnRestriction, QueryFactory queryFactory,
            AttributeRepository repository, String sqlUser, boolean negate) {
        StringPath table = AttrRepoUtils.getTablePath(repository, transaction);
        BooleanExpression productPredicate = getProductPredicate(txnRestriction.getProductId());
        BooleanExpression datePredicate = getDatePredicate(txnRestriction.getTimeFilter());
        BooleanExpression validPurchasePredicate = makeValidPurchase(txnRestriction);
        BooleanExpression predicate = productPredicate;
        if (datePredicate != null) {
            predicate = productPredicate.and(datePredicate);
        }
        if (validPurchasePredicate != null) {
            predicate = predicate.and(validPurchasePredicate);
        }
        BooleanExpression aggPredicate = getAggPredicate(txnRestriction);

        SQLQuery<?> query;
        SubQuery subQuery = new SubQuery();
        if (!negate) {
            query = queryFactory.getQuery(repository, sqlUser) //
                    .select(accountId) //
                    .from(table) //
                    .where(predicate);
            if (aggPredicate != null) {
                query = query.groupBy(accountId).having(aggPredicate);
            }
        } else {
            StringPath lhsPath = Expressions.stringPath(TranslatorUtils.generateAlias("lhs"));
            StringPath lhsId = Expressions.stringPath(lhsPath, InterfaceName.AccountId.name());
            StringPath rhsPath = Expressions.stringPath(TranslatorUtils.generateAlias("rhs"));
            StringPath rhsId = Expressions.stringPath(rhsPath, InterfaceName.AccountId.name());
            SQLQuery<?> rhsQry = SQLExpressions //
                    .select(accountId) //
                    .from(table) //
                    .where(predicate);
            if (aggPredicate != null) {
                rhsQry = rhsQry.groupBy(accountId).having(aggPredicate);
            }
            query = SQLExpressions //
                    .select(lhsId) //
                    .from(table.as(lhsPath)) //
                    .leftJoin(rhsQry, rhsPath) //
                    .on(lhsId.eq(rhsId)) //
                    .where(rhsId.isNull());
        }

        subQuery.setSubQueryExpression(query);
        return subQuery;
    }

    private static BooleanExpression makeValidPurchase(TransactionRestriction txnRestriction) {
        AggregationFilter unitFilter = txnRestriction.getUnitFilter();
        AggregationFilter spendFilter = txnRestriction.getSpentFilter();
        if (unitFilter == null && spendFilter == null) {
            NumberPath amtAgg = Expressions.numberPath(Double.class, InterfaceName.TotalAmount.name());
            NumberPath qtyAgg = Expressions.numberPath(Double.class, InterfaceName.TotalQuantity.name());
            return amtAgg.gt(Expressions.asNumber(0)).or(qtyAgg.gt(Expressions.asNumber(0)));
        }
        return null;
    }

    private static BooleanExpression getProductPredicate(String productId) {
        String[] productIds = productId.split(",");
        StringPath productIdPath = Expressions.stringPath(InterfaceName.ProductId.name());
        if (productIds.length == 1) {
            return productIdPath.eq(productId);
        } else {
            return productIdPath.in(Arrays.asList(productIds));
        }
    }

    private static BooleanExpression getDatePredicate(TimeFilter timeFilter) {
        if (ComparisonType.EVER.equals(timeFilter.getRelation())) {
            return null;
        }
        if (!PeriodStrategy.Template.Date.name().equals(timeFilter.getPeriod())) {
            throw new UnsupportedOperationException(
                    "Can only translate Date period, but " + timeFilter.getPeriod() + " was given.");
        }

        StringPath datePath = Expressions.stringPath(InterfaceName.TransactionDate.name());

        List<Object> vals = timeFilter.getValues();
        BooleanExpression predicate1 = vals.get(0) == null ? null : datePath.goe((String) vals.get(0));
        BooleanExpression predicate2 = vals.get(1) == null ? null : datePath.loe((String) vals.get(1));
        return mergePredicates(predicate1, predicate2);
    }

    private static BooleanExpression mergePredicates(BooleanExpression... predicates) {
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

    private static BooleanExpression getAggPredicate(TransactionRestriction txnRestriction) {
        AggregationFilter spentFilter = txnRestriction.getSpentFilter();
        AggregationFilter unitFilter = txnRestriction.getUnitFilter();

        StringPath amtAgg = Expressions.stringPath(InterfaceName.TotalAmount.name());
        StringPath qtyAgg = Expressions.stringPath(InterfaceName.TotalQuantity.name());

        BooleanExpression aggrAmountPredicate = (spentFilter != null)
                ? translateAggregatePredicate(amtAgg, spentFilter, true) : null;
        BooleanExpression aggrQuantityPredicate = (unitFilter != null)
                ? translateAggregatePredicate(qtyAgg, unitFilter, true) : null;

        return mergePredicates(aggrAmountPredicate, aggrQuantityPredicate);
    }

}
