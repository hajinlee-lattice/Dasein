package com.latticeengines.query.evaluator.restriction;

import java.math.BigDecimal;
import java.util.List;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.DateValueLookup;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.TimeRestriction;
import com.latticeengines.query.evaluator.lookup.LookupResolver;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;

public class TimeResolver extends BaseRestrictionResolver<TimeRestriction> implements RestrictionResolver<TimeRestriction> {

    TimeResolver(RestrictionResolverFactory factory) {
        super(factory);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public BooleanExpression resolve(TimeRestriction restriction) {
        TimeFilter filter = restriction.getFilter();
        Lookup lhs = filter.getLhs();
        Lookup rhs;
        if (filter.getRelation().equals(ComparisonType.EVER)) {
            return Expressions.TRUE;
        } else if (filter.getRelation().equals(ComparisonType.IN_CURRENT_PERIOD)) {
            rhs = new DateValueLookup(0, filter.getPeriod());
        } else {
            BigDecimal offset = new BigDecimal(filter.getValues().get(0).toString()).negate();
            rhs = new DateValueLookup(offset, filter.getPeriod());
        }

        LookupResolver lhsResolver = lookupFactory.getLookupResolver(lhs.getClass());
        List<ComparableExpression> lhsPaths = lhsResolver.resolveForCompare(lhs);
        ComparableExpression lhsPath = lhsPaths.get(0);

        LookupResolver rhsResolver = lookupFactory.getLookupResolver(rhs.getClass());
        List<ComparableExpression> rhsPaths = rhsResolver.resolveForCompare(rhs);

        BooleanExpression booleanExpression;

        switch (filter.getRelation()) {
        case IN_CURRENT_PERIOD:
            booleanExpression = lhsPath.eq(rhsPaths.get(0));
            break;
        case PRIOR:
            booleanExpression = lhsPath.loe(rhsPaths.get(0));
            break;
        case WITHIN:
            booleanExpression = lhsPath.goe(rhsPaths.get(0));
            break;
        default:
            throw new LedpException(LedpCode.LEDP_37006, new String[] { filter.getRelation().toString() });
        }
        return booleanExpression;
    }
}
