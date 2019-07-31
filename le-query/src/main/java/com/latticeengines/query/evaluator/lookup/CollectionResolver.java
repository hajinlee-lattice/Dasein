package com.latticeengines.query.evaluator.lookup;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.CollectionLookup;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;

public class CollectionResolver extends BaseLookupResolver<CollectionLookup>
        implements LookupResolver<CollectionLookup> {

    CollectionResolver(AttributeRepository repository) {
        super(repository);
    }

    @Override
    public List<ComparableExpression<? extends Comparable<?>>> resolveForCompare(CollectionLookup lookup) {
        Collection<Object> collection = lookup.getValues();
        if (collection == null || collection.isEmpty()) {
            throw new QueryEvaluationException("Collection lookup must have at least one element.");
        }
        return collection.stream().map(this::resolveValue).collect(Collectors.toList());
    }

    private ComparableExpression<? extends Comparable<?>> resolveValue(Object val) {
        ComparableExpression<? extends Comparable<?>> expression;
        if (val instanceof Integer) {
            expression = Expressions.asComparable((Integer) val);
        } else if (val instanceof Long) {
            expression = Expressions.asComparable((Long) val);
        } else if (val instanceof Double) {
            expression = Expressions.asComparable((Double) val);
        } else if (val instanceof Float) {
            expression = Expressions.asComparable((Float) val);
        } else {
            expression = Expressions.asComparable(val.toString());
        }
        return expression;
    }

    @Override
    public List<ComparableExpression<? extends Comparable<?>>> resolveForLowercaseCompare(CollectionLookup lookup) {
        Collection<Object> collection = lookup.getValues();
        if (collection == null || collection.isEmpty()) {
            throw new QueryEvaluationException("Collection lookup must have at least one element.");
        }
        return collection.stream().map(obj -> Expressions.asComparable(obj.toString().toLowerCase()))
                .collect(Collectors.toList());
    }

    @Override
    public Expression<?> resolveForSelect(CollectionLookup lookup, boolean asAlias) {
        throw new UnsupportedOperationException("Should not use collection lookup in select.");
    }
}
