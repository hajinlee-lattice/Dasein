package com.latticeengines.query.exposed.translator;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.ProductId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.TotalAmount;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.TotalQuantity;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.TransactionDate;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Transaction;

import java.util.Arrays;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.Restriction;
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

    public Restriction convert(TransactionRestriction txnRestriction, QueryFactory queryFactory,
                               AttributeRepository repository) {
        SubQuery subQuery = constructSubQuery(txnRestriction, queryFactory, repository);

        Restriction accountInRestriction = Restriction.builder() //
                .let(BusinessEntity.Account, InterfaceName.AccountId.name()) //
                .inSubquery(subQuery) //
                .build();

        return txnRestriction.isNegate() ? Restriction.builder().not((ConcreteRestriction) accountInRestriction).build()
                : accountInRestriction;
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
