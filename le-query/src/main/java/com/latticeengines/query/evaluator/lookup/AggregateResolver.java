package com.latticeengines.query.evaluator.lookup;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;

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
        throw new UnsupportedOperationException("Does not support aggregator in where clause yet.");
    }

    @SuppressWarnings("unchecked")
    @Override
    public Expression<?> resolveForSelect(AggregateLookup lookup, boolean asAlias) {
        switch (lookup.getAggregator()) {
            case COUNT:
                return countExpression(lookup, asAlias);
            default:
                throw new RuntimeException("Unknown aggregator");
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
