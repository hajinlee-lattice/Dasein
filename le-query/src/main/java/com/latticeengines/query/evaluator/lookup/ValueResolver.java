package com.latticeengines.query.evaluator.lookup;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

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
    public List<ComparableExpression<? extends Comparable<?>>> resolveForLowercaseCompare(T lookup) {
        return Collections.singletonList(Expressions.asComparable(lookup.getValue().toString().toLowerCase()));
    }

    @Override
    public Expression<?> resolveForSelect(ValueLookup lookup, boolean asAlias) {
        Object val = lookup.getValue();
        if (asAlias && StringUtils.isNotBlank(lookup.getAlias())) {
            return Expressions.as(Expressions.constant(val), lookup.getAlias());
        } else {
            return Expressions.constant(val);
        }
    }
}
