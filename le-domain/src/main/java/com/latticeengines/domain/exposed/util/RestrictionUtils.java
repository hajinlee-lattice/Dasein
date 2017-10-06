package com.latticeengines.domain.exposed.util;

import static com.latticeengines.domain.exposed.query.ComparisonType.IS_NOT_NULL;
import static com.latticeengines.domain.exposed.query.ComparisonType.IS_NULL;

import java.util.List;

import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Restriction;

public class RestrictionUtils {

    public static Restriction convertValueComparison(Lookup attr, ComparisonType comparisonType, Object value) {
        Restriction restriction = null;
        switch (comparisonType) {
        case EQUAL:
            restriction = Restriction.builder().let(attr).eq(value).build();
            break;
        case NOT_EQUAL:
            restriction = Restriction.builder().let(attr).neq(value).build();
            break;
        case GREATER_THAN:
            restriction = Restriction.builder().let(attr).gt(value).build();
            break;
        case GREATER_OR_EQUAL:
            restriction = Restriction.builder().let(attr).gte(value).build();
            break;
        case LESS_THAN:
            restriction = Restriction.builder().let(attr).lt(value).build();
            break;
        case LESS_OR_EQUAL:
            restriction = Restriction.builder().let(attr).lte(value).build();
            break;
        default:
            throw new UnsupportedOperationException("comparator " + comparisonType + " is not supported yet");
        }
        return restriction;
    }

    public static Restriction convertValueComparisons(Lookup attr, ComparisonType comparisonType, List<Object> values) {
        Restriction restriction = null;
        switch (comparisonType) {
            case IS_NULL:
                restriction = new ConcreteRestriction(false, attr, IS_NULL, null);
                break;
            case IS_NOT_NULL:
                restriction = new ConcreteRestriction(false, attr, IS_NOT_NULL, null);
                break;
            case EQUAL:
                validateSingleValue(values);
                restriction = convertValueComparison(attr, comparisonType, values.get(0));
                break;
            case NOT_EQUAL:
                validateSingleValue(values);
                restriction = convertValueComparison(attr, comparisonType, values.get(0));
                break;
            case GREATER_THAN:
                validateSingleValue(values);
                restriction = convertValueComparison(attr, comparisonType, values.get(0));
                break;
            case GREATER_OR_EQUAL:
                validateSingleValue(values);
                restriction = convertValueComparison(attr, comparisonType, values.get(0));
                break;
            case LESS_THAN:
                validateSingleValue(values);
                restriction = convertValueComparison(attr, comparisonType, values.get(0));
                break;
            case LESS_OR_EQUAL:
                validateSingleValue(values);
                restriction = convertValueComparison(attr, comparisonType, values.get(0));
                break;
            case IN_RANGE:
            case GTE_AND_LTE:
            case GT_AND_LTE:
            case GTE_AND_LT:
            case GT_AND_LT:
                validateInRangeValues(values);
                Object min = values.get(0);
                Object max = values.get(1);
                switch (comparisonType) {
                    case GTE_AND_LTE:
                        restriction = Restriction.builder().and( //
                                Restriction.builder().let(attr).gte(min).build(),
                                Restriction.builder().let(attr).lte(max).build()).build();
                        break;
                    case GT_AND_LTE:
                        restriction = Restriction.builder().and( //
                                Restriction.builder().let(attr).gt(min).build(),
                                Restriction.builder().let(attr).lte(max).build()).build();
                        break;
                    case GTE_AND_LT:
                        restriction = Restriction.builder().and( //
                                Restriction.builder().let(attr).gte(min).build(),
                                Restriction.builder().let(attr).lt(max).build()).build();
                        break;
                    case GT_AND_LT:
                        restriction = Restriction.builder().and( //
                                Restriction.builder().let(attr).gt(min).build(),
                                Restriction.builder().let(attr).lt(max).build()).build();
                        break;
                    default:
                        restriction = Restriction.builder().let(attr).in(min, max).build();
                        break;
                }
                break;
            case IN_COLLECTION:
                restriction = Restriction.builder().let(attr).inCollection(values).build();
                break;
            case CONTAINS:
                restriction = Restriction.builder().let(attr).contains(values.get(0)).build();
                break;
            case NOT_CONTAINS:
                restriction = Restriction.builder().let(attr).notcontains(values.get(0)).build();
                break;
            case STARTS_WITH:
                restriction = Restriction.builder().let(attr).not().startsWith(values.get(0)).build();
                break;
            default:
                throw new UnsupportedOperationException("comparator " + comparisonType + " is not supported yet");
        }
        return restriction;
    }

    private static void validateSingleValue(List<Object> values) {
        if (values == null || values.size() != 1) {
            throw new IllegalArgumentException("collection should have one value");
        }
    }

    private static void validateInRangeValues(List<Object> values) {
        if (values == null || values.size() != 2) {
            throw new IllegalArgumentException("range should contain both min and max value");
        }
    }

}
