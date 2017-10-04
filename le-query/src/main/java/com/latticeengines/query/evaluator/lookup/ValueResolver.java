package com.latticeengines.query.evaluator.lookup;

import java.util.Collections;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.ValueLookup;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;

public class ValueResolver<T extends ValueLookup> extends BaseLookupResolver<T> implements LookupResolver<T> {

    ValueResolver(AttributeRepository repository) {
        super(repository);
    }

    @Override
    public List<ComparableExpression<? extends Comparable<?>>> resolveForCompare(ValueLookup lookup) {
        return Collections.singletonList(Expressions.asComparable(lookup.getValue().toString()));
    }

    @Override
    public Expression<?> resolveForSelect(ValueLookup lookup, boolean asAlias) {
        throw new UnsupportedOperationException("Should not use value lookup in select.");
    }
}
