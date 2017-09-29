package com.latticeengines.query.evaluator.restriction;

import java.util.List;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.ValueLookup;
import com.latticeengines.query.evaluator.lookup.LookupResolver;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;

public class TimeResolver extends BaseRestrictionResolver<TimeFilter> implements RestrictionResolver<TimeFilter> {

    TimeResolver(RestrictionResolverFactory factory) {
        super(factory);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public BooleanExpression resolve(TimeFilter restriction) {
        Lookup lhs = restriction.getLhs();
        Lookup rhs;
        if (restriction.getRelation().equals(ComparisonType.EVER)) {
            rhs = new ValueLookup(0);
        } else {
            rhs = new ValueLookup(restriction.getValues().get(0));
        }

        LookupResolver lhsResolver = lookupFactory.getLookupResolver(lhs.getClass());
        List<ComparableExpression> lhsPaths = lhsResolver.resolveForTimeCompare(lhs);
        ComparableExpression lhsPath = lhsPaths.get(0);

        LookupResolver rhsResolver = lookupFactory.getLookupResolver(rhs.getClass());
        List<ComparableExpression> rhsPaths = rhsResolver.resolveForTimeCompare(rhs);

        BooleanExpression booleanExpression;

        switch (restriction.getRelation()) {
        case EVER:
            booleanExpression = Expressions.TRUE;
            break;
        case IN_CURRENT_PERIOD:

        case BEFORE:
            booleanExpression = lhsPath.lt(rhsPaths.get(0));
            break;
        case AFTER:
            booleanExpression = lhsPath.gt(rhsPaths.get(0));
            break;
        default:
            throw new LedpException(LedpCode.LEDP_37006, new String[] { restriction.getRelation().toString() });
        }
        return booleanExpression;
    }
}
