package com.latticeengines.query.evaluator.restriction;

import java.util.stream.Collectors;

import com.latticeengines.domain.exposed.query.LogicalOperator;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.Expressions;

public class LogicalResolver extends BaseRestrictionResolver<LogicalRestriction>
        implements RestrictionResolver<LogicalRestriction> {

    LogicalResolver(RestrictionResolverFactory factory) {
        super(factory);
    }

    @SuppressWarnings("unchecked")
    @Override
    public BooleanExpression resolve(LogicalRestriction restriction) {
        BooleanExpression[] childExpressions = new BooleanExpression[restriction.getRestrictions().size()];
        childExpressions = restriction.getRestrictions().stream() //
                .map(r -> {
                    RestrictionResolver resolver = factory.getRestrictionResolver(r.getClass());
                    return resolver.resolve(r);
                }) //
                .collect(Collectors.toList()) //
                .toArray(childExpressions);
        if (restriction.getOperator() == LogicalOperator.AND) {
            return Expressions.allOf(childExpressions);
        } else {
            return Expressions.anyOf(childExpressions);
        }
    }

}
