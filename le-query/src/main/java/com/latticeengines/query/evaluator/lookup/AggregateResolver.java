package com.latticeengines.query.evaluator.lookup;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.CaseLookup;
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
import com.latticeengines.query.util.QueryUtils;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.NumberExpression;
import com.querydsl.core.types.dsl.NumberPath;

public class AggregateResolver extends BaseLookupResolver<AggregateLookup> implements LookupResolver<AggregateLookup> {

    private LookupResolverFactory factory;

    public AggregateResolver(AttributeRepository repository, LookupResolverFactory factory) {
        super(repository);
        this.factory = factory;
    }

    @Override
    public List<ComparableExpression<? extends Comparable<?>>> resolveForCompare(AggregateLookup lookup) {
        switch (lookup.getAggregator()) {
        case SUM:
        case AVG:
        case MAX:
        case MIN:
            return numExpressionForCompare(lookup);
        case COUNT:
            return Collections.singletonList(Expressions.asComparable(countExpression(lookup, true)));
        default:
            throw new UnsupportedOperationException(
                    "Does not support aggregator " + lookup.getAggregator() + "in where clause yet.");
        }
    }

    @Override
    public Expression<?> resolveForSelect(AggregateLookup lookup, boolean asAlias) {
        switch (lookup.getAggregator()) {
        case COUNT:
            return countExpression(lookup, asAlias);
        case SUM:
        case AVG:
        case MAX:
        case MIN:
            return numExpressionForSelect(lookup, asAlias);
        default:
            throw new RuntimeException("Unsupported aggregator " + lookup.getAggregator());
        }
    }

    private List<ComparableExpression<? extends Comparable<?>>> numExpressionForCompare(AggregateLookup lookup) {
        NumberExpression<?> numExpression = (NumberExpression<?>) numExpressionForSelect(lookup, false);
        return Collections.singletonList(Expressions.asComparable(numExpression));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Expression<?> numExpressionForSelect(AggregateLookup lookup, boolean asAlias) {
        if (lookup.getLookup() == null) {
            throw new RuntimeException("Sum aggregation cannot be applied for empty lookup.");
        }

        NumberPath numberPath = null;
        NumberExpression numberExpression = null;
        if (lookup.getLookup() instanceof AttributeLookup) {
            AttributeLookup innerLookup = (AttributeLookup) lookup.getLookup();
            ColumnMetadata cm = getColumnMetadata(innerLookup);
            numberPath = QueryUtils.getAttributeNumberPath(innerLookup.getEntity(), cm.getAttrName());
        } else if (lookup.getLookup() instanceof SubQueryAttrLookup) {
            SubQueryAttrLookup innerLookup = (SubQueryAttrLookup) lookup.getLookup();
            numberPath = QueryUtils.getAttributeNumberPath(innerLookup.getSubQuery(), innerLookup.getAttribute());
        } else if (lookup.getLookup() instanceof CaseLookup) {
            CaseLookup caseLookup = (CaseLookup) lookup.getLookup();
            LookupResolver resolver = factory.getLookupResolver(caseLookup.getClass());
            Expression<BigDecimal> expression = resolver.resolveForSelect(caseLookup, false);
            numberExpression = Expressions.asNumber(expression);
        }

        if (numberPath == null && numberExpression == null) {
            throw new RuntimeException(
                    "Sum aggregation is not supported for " + lookup.getLookup().getClass().getName());
        }

        if (asAlias && StringUtils.isNotBlank(lookup.getAlias())) {
            switch (lookup.getAggregator()) {
            case SUM:
                if (numberPath != null) {
                    return numberPath.sum().coalesce(lookup.getNvl()).asNumber().as(lookup.getAlias());
                } else {
                    return numberExpression.sum().coalesce(lookup.getNvl()).asNumber().as(lookup.getAlias());
                }
            case AVG:
                if (numberPath != null) {
                    return numberPath.avg().coalesce(lookup.getNvl()).asNumber().as(lookup.getAlias());
                } else {
                    return numberExpression.avg().coalesce(lookup.getNvl()).asNumber().as(lookup.getAlias());
                }
            case MAX:
                if (numberPath != null) {
                    return numberPath.max().as(lookup.getAlias());
                } else {
                    return numberExpression.max().as(lookup.getAlias());
                }
            case MIN:
                if (numberPath != null) {
                    return numberPath.min().as(lookup.getAlias());
                } else {
                    return numberExpression.min().as(lookup.getAlias());
                }
            default:
                throw new UnsupportedOperationException("Aggregator " + lookup.getAggregator() + " is not supported");
            }
        } else {
            switch (lookup.getAggregator()) {
            case SUM:
                if (numberPath != null) {
                    return numberPath.sum().coalesce(lookup.getNvl()).asNumber();
                } else {
                    return numberExpression.sum().coalesce(lookup.getNvl()).asNumber();
                }
            case AVG:
                if (numberPath != null) {
                    return numberPath.avg().coalesce(lookup.getNvl()).asNumber();
                } else {
                    return numberExpression.avg().coalesce(lookup.getNvl()).asNumber();
                }
            case MAX:
                if (numberPath != null) {
                    return numberPath.max();
                } else {
                    return numberExpression.max();
                }
            case MIN:
                if (numberPath != null) {
                    return numberPath.min();
                } else {
                    return numberExpression.min();
                }
            default:
                throw new UnsupportedOperationException("Aggregator " + lookup.getAggregator() + " is not supported");
            }
        }
    }

    private Expression<? extends Comparable<?>> countExpression(AggregateLookup lookup, boolean asAlias) {
        if (asAlias && StringUtils.isNotBlank(lookup.getAlias())) {
            return Expressions.asNumber(1).count().as(lookup.getAlias());
        } else {
            return Expressions.asNumber(1).count();
        }
    }

}
