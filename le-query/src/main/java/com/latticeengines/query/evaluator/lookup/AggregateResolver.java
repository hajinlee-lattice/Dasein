package com.latticeengines.query.evaluator.lookup;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
import com.latticeengines.query.util.QueryUtils;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.NumberExpression;
import com.querydsl.core.types.dsl.NumberPath;
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.core.types.dsl.StringPath;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;

public class AggregateResolver extends BaseLookupResolver<AggregateLookup>
        implements LookupResolver<AggregateLookup> {

    private LookupResolverFactory factory;

    public AggregateResolver(AttributeRepository repository, LookupResolverFactory factory) {
        super(repository);
        this.factory = factory;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ComparableExpression<String>> resolveForCompare(AggregateLookup lookup) {
        throw new UnsupportedOperationException("Does not support aggregator in where clause.");
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ComparableExpression<Comparable>> resolveForAggregateCompare(AggregateLookup lookup) {
        switch (lookup.getAggregator()) {
            case SUM:
                return sumExpressionForCompare(lookup);
            default:
                throw new UnsupportedOperationException(
                        "Does not support aggregator " + lookup.getAggregator() + "in where clause yet."
                );
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Expression<?> resolveForSelect(AggregateLookup lookup, boolean asAlias) {
        switch (lookup.getAggregator()) {
            case COUNT:
                return countExpression(lookup, asAlias);
            case SUM:
                return sumExpressionForSelect(lookup, asAlias);
            default:
                throw new RuntimeException("Unsupported aggregator " + lookup.getAggregator());
        }
    }

    @SuppressWarnings("unchecked")
    private List<ComparableExpression<Comparable>> sumExpressionForCompare(AggregateLookup lookup) {
        NumberExpression sumExpression = (NumberExpression) sumExpressionForSelect(lookup, false);
        return Collections.singletonList(Expressions.asComparable(sumExpression));
    }

    private Expression<?> sumExpressionForSelect(AggregateLookup lookup, boolean asAlias) {
        if (lookup.getLookup() == null) {
            throw new RuntimeException("Sum aggregation cannot be applied for empty lookup.");
        }

        NumberPath numberPath = null;
        if (lookup.getLookup() instanceof AttributeLookup) {
            AttributeLookup innerLookup = (AttributeLookup) lookup.getLookup();
            ColumnMetadata cm = getColumnMetadata(innerLookup);
            if (cm.getStats() != null) {
                throw new RuntimeException("Sum aggregation is only supported for non-bucketed attribute");
            }
            numberPath = QueryUtils.getAttributeNumberPath(innerLookup.getEntity(), cm.getName());
        } else if (lookup.getLookup() instanceof SubQueryAttrLookup) {
            SubQueryAttrLookup innerLookup = (SubQueryAttrLookup) lookup.getLookup();
            numberPath = QueryUtils.getAttributeNumberPath(innerLookup.getSubQuery(), innerLookup.getAttribute());
        }

        if (numberPath == null) {
            throw new RuntimeException("Sum aggregation is not supported for " +
                    lookup.getLookup().getClass().getName());
        }

        if (asAlias && StringUtils.isNotBlank(lookup.getAlias())) {
            return numberPath.sum().as(lookup.getAlias());
        } else {
            return numberPath.sum();
        }
    }

    private Expression<?> countExpression(AggregateLookup lookup, boolean asAlias) {
        if (asAlias && StringUtils.isNotBlank(lookup.getAlias())) {
            return Expressions.asNumber(1).count().as(lookup.getAlias());
        } else {
            return Expressions.asNumber(1).count();
        }
    }

}
