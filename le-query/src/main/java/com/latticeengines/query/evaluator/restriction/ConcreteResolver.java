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
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
import com.latticeengines.domain.exposed.query.ValueLookup;
import com.latticeengines.query.evaluator.QueryProcessor;
import com.latticeengines.query.evaluator.lookup.LookupResolver;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
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
            if (restriction.getRelation().equals(ComparisonType.IS_NULL)) {
                // is null means bktId = 0
                restriction.setRelation(ComparisonType.EQUAL);
                rhs = new ValueLookup(0);
            } else {
                ValueLookup valueLookup = (ValueLookup) rhs;
                AttributeStats stats = findAttributeStats(attrLookup);
                Buckets bkts = stats.getBuckets();
                rhs = convert(bkts, (String) valueLookup.getValue());
            }
        }

        LookupResolver lhsResolver = lookupFactory.getLookupResolver(lhs.getClass());
        List<ComparableExpression<String>> lhsPaths = lhsResolver.resolveForCompare(lhs);
        ComparableExpression<String> lhsPath = lhsPaths.get(0);

        if (restriction.getRelation().equals(ComparisonType.EQUAL) && isNullValueLookup(rhs)) {
            if (restriction.getNegate()) {
                restriction.setRelation(ComparisonType.IS_NOT_NULL);
            } else {
                restriction.setRelation(ComparisonType.IS_NULL);
            }
        }
        
        if (restriction.getRelation().equals(ComparisonType.IS_NULL)) {
            return lhsPath.isNull();
        } else if (restriction.getRelation().equals(ComparisonType.IS_NOT_NULL)) {
            return lhsPath.isNotNull();
        } else {
            LookupResolver rhsResolver = lookupFactory.getLookupResolver(rhs.getClass());
            List<ComparableExpression<String>> rhsPaths = rhsResolver.resolveForCompare(rhs);

            BooleanExpression booleanExpression;

            switch (restriction.getRelation()) {
            case EQUAL:
                booleanExpression = lhsPath.eq(rhsPaths.get(0));
                break;
            case GREATER_OR_EQUAL:
                booleanExpression = lhsPath.goe(rhsPaths.get(0));
                break;
            case GREATER_THAN:
                booleanExpression = lhsPath.gt(rhsPaths.get(0));
                break;
            case LESS_OR_EQUAL:
                booleanExpression = lhsPath.loe(rhsPaths.get(0));
                break;
            case LESS_THAN:
                booleanExpression = lhsPath.lt(rhsPaths.get(0));
                break;
            case IN_RANGE:
                if (rhsPaths.size() > 1) {
                    booleanExpression = lhsPath.between(rhsPaths.get(0), rhsPaths.get(1));
                } else {
                    booleanExpression = lhsPath.eq(rhsPaths.get(0));
                }
                break;
            case IN_COLLECTION:
                if (rhs instanceof SubQueryAttrLookup) {
                    booleanExpression = lhsPaths.get(0).in(rhsPaths.get(0));
                } else {
                    // when there's only 1 element in the collection, querydsl generates something
                    // like "attr in ?", which is not a valid syntax so we treat it differently
                    if (rhsPaths.size() > 1) {
                        booleanExpression = lhsPath.in(rhsPaths.toArray(new ComparableExpression[0]));
                    } else {
                        booleanExpression = lhsPath.eq(rhsPaths.get(0));
                    }
                }
                break;
            default:
                throw new LedpException(LedpCode.LEDP_37006, new String[] { restriction.getRelation().toString() });
            }

            if (restriction.getNegate()) {
                return booleanExpression.not();
            } else {
                return booleanExpression;
            }
        }
    }

    private boolean isNullValueLookup(Lookup lookup) {
        return lookup instanceof ValueLookup && ((ValueLookup) lookup).getValue() == null;
    }

    private boolean isBucket(ConcreteRestriction restriction) {
        Lookup lhs = restriction.getLhs();
        if (lhs instanceof AttributeLookup) {
            AttributeLookup attrLookup = (AttributeLookup) lhs;
            ColumnMetadata cm = findAttributeMetadata(attrLookup);
            if (cm == null) {
                throw new IllegalArgumentException("Cannot find metadata for attribute " + attrLookup + " in attr repo.");
            }
            if (cm.getBitOffset() != null) {
                // lhs is bit encoded
                if (restriction.getRelation().equals(ComparisonType.IS_NULL)) {
                    return true;
                }
                Lookup rhs = restriction.getRhs();
                if (rhs != null && rhs instanceof ValueLookup) {
                    ValueLookup valueLookup = (ValueLookup) rhs;
                    Object val = valueLookup.getValue();
                    if (val == null || (val instanceof String)) {
                        if (restriction.getNegate()) {
                            throw new UnsupportedOperationException("Not support negate on bucketed attribute.");
                        }
                        if (!ComparisonType.EQUAL.equals(restriction.getRelation())) {
                            throw new UnsupportedOperationException(
                                    "Only support ComparisonType.EQUAL on bucketed attribute.");
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
