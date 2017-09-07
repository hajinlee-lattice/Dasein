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
    public List<ComparableExpression<String>> resolveForCompare(CollectionLookup lookup) {
        Collection<Object> collection = lookup.getValues();
        if (collection == null || collection.isEmpty()) {
            throw new QueryEvaluationException("Collection lookup must have at least one element.");
        }
        return collection.stream().map(obj -> Expressions.asComparable(obj.toString())).collect(Collectors.toList());
    }

    @Override
    public Expression<?> resolveForSelect(CollectionLookup lookup, boolean asAlias) {
        throw new UnsupportedOperationException("Should not use collection lookup in select.");
    }
}
