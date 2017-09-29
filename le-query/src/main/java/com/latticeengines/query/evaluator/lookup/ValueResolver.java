package com.latticeengines.query.evaluator.lookup;

import java.util.Collections;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.ValueLookup;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;

public class ValueResolver extends BaseLookupResolver<ValueLookup> implements LookupResolver<ValueLookup> {

    ValueResolver(AttributeRepository repository) {
        super(repository);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<ComparableExpression<? extends Comparable>> resolveForCompare(ValueLookup lookup) {
        return Collections.singletonList(Expressions.asComparable(lookup.getValue().toString()));
    }

    @Override
    public Expression<?> resolveForSelect(ValueLookup lookup, boolean asAlias) {
        throw new UnsupportedOperationException("Should not use value lookup in select.");
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<ComparableExpression<? extends Comparable>> resolveForTimeCompare(ValueLookup lookup) {
        DateTime dt = ISODateTimeFormat.dateElementParser().parseDateTime(lookup.getValue().toString());
        return Collections.singletonList(Expressions.asDate(dt.toDate()));
    }
}
