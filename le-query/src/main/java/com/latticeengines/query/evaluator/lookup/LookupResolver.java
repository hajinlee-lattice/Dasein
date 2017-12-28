package com.latticeengines.query.evaluator.lookup;

import java.util.List;

import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.sql.SQLQuery;
import com.querydsl.sql.Union;

public interface LookupResolver<T extends Lookup> {
    default Union<?> resolveForUnion(T lookup) {
        throw new UnsupportedOperationException("Resolve for union is not supported yet");
    }

    default SQLQuery<?> resolveForFrom(T lookup) {
        throw new UnsupportedOperationException("Resolve for from is not supported yet");
    }

    default ComparableExpression<String> resolveForSubselect(T lookup) {
        throw new UnsupportedOperationException("Resolve for subselect is not supported yet");
    }

    default List<ComparableExpression<? extends Comparable<?>>> resolveForCompare(T lookup) {
        throw new UnsupportedOperationException("Resolve for compare is not supported yet");
    }

    default List<ComparableExpression<? extends Comparable<?>>> resolveForLowercaseCompare(T lookup) {
        return resolveForCompare(lookup);
    }


    default Expression<?> resolveForSelect(T lookup, boolean asAlias) {
        throw new UnsupportedOperationException("Resolve for select is not supported yet");
    }

}
