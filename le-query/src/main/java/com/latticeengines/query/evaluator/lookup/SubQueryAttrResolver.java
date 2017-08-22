package com.latticeengines.query.evaluator.lookup;

import java.util.Collections;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
import com.latticeengines.query.util.QueryUtils;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;


public class SubQueryAttrResolver extends BaseLookupResolver<SubQueryAttrLookup>
        implements LookupResolver<SubQueryAttrLookup> {

    public SubQueryAttrResolver(AttributeRepository repository) {
        super(repository);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ComparableExpression<String>> resolveForCompare(SubQueryAttrLookup lookup) {
        return Collections
                .singletonList(Expressions.asComparable((Expression<String>) resolveForSelect(lookup, false)));
    }

    @Override
    public Expression<?> resolveForSelect(SubQueryAttrLookup lookup, boolean asAlias) {
        return QueryUtils.getAttributePath(lookup.getSubQuery(), lookup.getAttribute());
    }

}
