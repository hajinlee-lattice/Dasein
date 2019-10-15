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
        Object val = lookup.getValue();
        ComparableExpression<? extends Comparable<?>> expression;
        if (val instanceof Boolean) {
            expression = Expressions.asComparable((Boolean) val);
        } else if (val instanceof Integer) {
            expression = Expressions.asComparable((Integer) val);
        } else if (val instanceof Long) {
            expression = Expressions.asComparable((Long) val);
        } else if (val instanceof Double) {
            expression = Expressions.asComparable((Double) val);
        } else if (val instanceof Float) {
            expression = Expressions.asComparable((Float) val);
        } else {
            expression = Expressions.asComparable(lookup.getValue().toString());
        }
        return Collections.singletonList(expression);
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
