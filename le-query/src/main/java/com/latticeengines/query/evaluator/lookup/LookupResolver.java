package com.latticeengines.query.evaluator.lookup;

import java.util.List;

import com.latticeengines.domain.exposed.query.Lookup;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.ComparableExpression;

public interface LookupResolver<T extends Lookup> {
    default List<ComparableExpression<Comparable>> resolveForAggregateCompare(T lookup) {
        throw new UnsupportedOperationException("Resolve for aggregate compare not supported yet");
    }

    default List<ComparableExpression<String>> resolveForCompare(T lookup) {
        throw new UnsupportedOperationException("Resolve for compare is not supported yet");
    }

    default Expression<?> resolveForSelect(T lookup, boolean asAlias) {
        throw new UnsupportedOperationException("Resolve for select is not supported yet");
    }
}
