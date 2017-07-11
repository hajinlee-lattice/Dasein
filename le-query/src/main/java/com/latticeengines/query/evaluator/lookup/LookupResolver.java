package com.latticeengines.query.evaluator.lookup;

import java.util.List;

import com.latticeengines.domain.exposed.query.Lookup;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.ComparableExpression;

public interface LookupResolver<T extends Lookup> {

    List<ComparableExpression<String>> resolveForCompare(T lookup);

    Expression<?> resolveForSelect(T lookup, boolean asAlias);
}
