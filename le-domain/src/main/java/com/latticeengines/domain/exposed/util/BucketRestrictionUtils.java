package com.latticeengines.domain.exposed.util;

import static com.latticeengines.domain.exposed.query.ComparisonType.EQUAL;
import static com.latticeengines.domain.exposed.query.ComparisonType.GREATER_OR_EQUAL;
import static com.latticeengines.domain.exposed.query.ComparisonType.LESS_THAN;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.RangeLookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.ValueLookup;

public class BucketRestrictionUtils {
    public static BucketRestriction from(Restriction restriction) {
        return from((ConcreteRestriction) restriction);
    }

    // from backend to UI
    private static BucketRestriction from(ConcreteRestriction restriction) {
        if (restriction.getNegate()) {
            throw new IllegalArgumentException("Negate concrete restriction cannot be converted to bucket.");
        }
        AttributeLookup attributeLookup = (AttributeLookup) restriction.getLhs();
        Bucket bucket = new Bucket();
        BucketRestriction bucketRestriction = new BucketRestriction();
        bucketRestriction.setAttr(attributeLookup);
        bucketRestriction.setBkt(bucket);
        ComparisonType operator = restriction.getRelation();
        Lookup rhs = restriction.getRhs();
        switch (operator) {
        case EQUAL:
        case GREATER_OR_EQUAL:
        case LESS_THAN:
            if (rhs instanceof ValueLookup) {
                return updateForValueLookup(bucketRestriction, operator, (ValueLookup) rhs);
            } else {
                throw new IllegalArgumentException("The right hand side of EQUAL must be a value.");
            }
        case IN_RANGE:
            if (rhs instanceof RangeLookup) {
                RangeLookup rangeLookup = (RangeLookup) rhs;
                Object min = rangeLookup.getMin();
                Object max = rangeLookup.getMax();
                Pair<Object, Object> range = ImmutablePair.of(min, max);
                bucket.setRange(range);
                return bucketRestriction;
            } else {
                throw new IllegalArgumentException("Cannot convert to bucket.");
            }
        default:
            throw new IllegalArgumentException("Cannot convert " + operator);
        }
    }

    private static BucketRestriction updateForValueLookup(BucketRestriction restriction, ComparisonType operator,
            ValueLookup valueLookup) {
        Object val = valueLookup.getValue();
        if (EQUAL.equals(operator)) {
            if (val != null && val instanceof String) {
                restriction.getBkt().setLabel((String) val);
                return restriction;
            } else if (val == null) {
                return restriction;
            } else {
                throw new IllegalArgumentException("Cannot handle non string value.");
            }
        } else if (GREATER_OR_EQUAL.equals(operator)) {
            if (val != null && val instanceof Comparable) {
                restriction.getBkt().setRange(Pair.of(val, null));
                return restriction;
            } else {
                throw new IllegalArgumentException(
                        "Cannot convert greater than or equal to for the given value: " + val);
            }
        } else if (LESS_THAN.equals(operator)) {
            if (val != null && val instanceof Comparable) {
                restriction.getBkt().setRange(Pair.of(null, val));
                return restriction;
            } else {
                throw new IllegalArgumentException("Cannot convert less than for for the given value: " + val);
            }
        } else {
            throw new UnsupportedOperationException("Only support " + EQUAL + ", " + GREATER_OR_EQUAL + " and "
                    + LESS_THAN + " for value lookup reverse translation");
        }
    }

}
