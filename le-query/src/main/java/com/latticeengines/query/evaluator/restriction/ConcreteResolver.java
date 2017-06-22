package com.latticeengines.query.evaluator.restriction;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.ValueLookup;
import com.latticeengines.query.evaluator.lookup.LookupResolver;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.latticeengines.query.util.AttrRepoUtils;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.ComparableExpression;

public class ConcreteResolver extends BaseRestrictionResolver<ConcreteRestriction>
        implements RestrictionResolver<ConcreteRestriction> {

    ConcreteResolver(RestrictionResolverFactory factory) {
        super(factory);
    }

    @SuppressWarnings("unchecked")
    @Override
    public BooleanExpression resolve(ConcreteRestriction restriction) {
        Lookup lhs = restriction.getLhs();
        Lookup rhs = restriction.getRhs();

        if (isBucket(restriction)) {
            AttributeLookup attrLookup = (AttributeLookup) lhs;
            ValueLookup valueLookup = (ValueLookup) rhs;
            AttributeStats stats = findAttributeStats(attrLookup);
            Buckets bkts = stats.getBuckets();
            rhs = convert(bkts, (String) valueLookup.getValue());
        }

        LookupResolver lhsResolver = lookupFactory.getLookupResolver(lhs.getClass());
        List<ComparableExpression<String>> lhsPaths = lhsResolver.resolveForCompare(lhs);
        ComparableExpression<String> lhsPath = lhsPaths.get(0);
        LookupResolver rhsResolver = lookupFactory.getLookupResolver(rhs.getClass());
        List<ComparableExpression<String>> rhsPaths = rhsResolver.resolveForCompare(rhs);

        switch (restriction.getRelation()) {
            case EQUAL:
                return lhsPath.eq(rhsPaths.get(0));
            case GREATER_OR_EQUAL:
                return lhsPath.goe(rhsPaths.get(0));
            case GREATER_THAN:
                return lhsPath.gt(rhsPaths.get(0));
            case LESS_OR_EQUAL:
                return lhsPath.loe(rhsPaths.get(0));
            case LESS_THAN:
                return lhsPath.lt(rhsPaths.get(0));
            case IN_RANGE:
                if (rhsPaths.size() > 1) {
                    return lhsPath.between(rhsPaths.get(0), rhsPaths.get(1));
                } else {
                    return lhsPath.eq(rhsPaths.get(0));
                }
            default:
                throw new LedpException(LedpCode.LEDP_37006, new String[] { restriction.getRelation().toString() });
        }
    }

    private boolean isBucket(ConcreteRestriction restriction) {
        Lookup lhs = restriction.getLhs();
        Lookup rhs = restriction.getRhs();
        if ((lhs instanceof AttributeLookup) && !(rhs instanceof AttributeLookup)) {
            AttributeLookup attrLookup = (AttributeLookup) lhs;
            ColumnMetadata cm = findAttributeMetadata(attrLookup);
            if (cm.getBitOffset() != null) {
                // lhs is bucketed attribute, but rhs is not
                if (rhs instanceof ValueLookup) {
                    ValueLookup valueLookup = (ValueLookup) rhs;
                    Object val = valueLookup.getValue();
                    if (val == null || (val instanceof String)) {
                        if (restriction.getNegate()) {
                            throw new UnsupportedOperationException("Not support negate on bucketed attribute.");
                        }
                        if (!ComparisonType.EQUAL.equals(restriction.getRelation())) {
                            throw new UnsupportedOperationException("Only support ComparisonType.EQUAL on bucketed attribute.");
                        }
                        return true;
                    } else {
                        throw new UnsupportedOperationException("Bucket attribute can only do string value lookup.");
                    }
                } else {
                    throw new UnsupportedOperationException("Bucket attribute can only do string value lookup.");
                }
            }
        }
        return false;
    }

    private ValueLookup convert(Buckets buckets, String bktLbl) {
        int value = 0;
        if (bktLbl != null) {
            Bucket bkt = buckets.getBucketList().stream() //
                    .filter(b -> b.getLabel().equals(bktLbl)) //
                    .findFirst().orElse(null);
            if (bkt == null) {
                throw new QueryEvaluationException("Cannot find label [" + bktLbl + "] in statistics.");
            }
            value = bkt.getIdAsInt();
        }
        return new ValueLookup(value);
    }

}
