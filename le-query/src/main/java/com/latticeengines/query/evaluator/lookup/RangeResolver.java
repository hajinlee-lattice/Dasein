package com.latticeengines.query.evaluator.lookup;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.RangeLookup;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.latticeengines.query.util.AttrRepoUtils;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;

public class RangeResolver extends BaseLookupResolver<RangeLookup> implements LookupResolver<RangeLookup> {

    RangeResolver(AttrRepoUtils attrRepoUtils, AttributeRepository repository) {
        super(attrRepoUtils, repository);
    }

    @Override
    public List<ComparableExpression<String>> resolveForCompare(RangeLookup lookup) {
        if (lookup.getMin() == null || lookup.getMax() == null) {
            throw new QueryEvaluationException("Range lookup must have both boundaries not null.");
        }
        List<ComparableExpression<String>> expressions = new ArrayList<>();
        ComparableExpression<String> min = Expressions.asComparable(lookup.getMin().toString());
        ComparableExpression<String> max = Expressions.asComparable(lookup.getMax().toString());
        expressions.add(min);
        expressions.add(max);
        return expressions;
    }

    @Override
    public Expression<?> resolveForSelect(RangeLookup lookup, boolean asAlias) {
        throw new UnsupportedOperationException("Should not use range lookup in select.");
    }

}
