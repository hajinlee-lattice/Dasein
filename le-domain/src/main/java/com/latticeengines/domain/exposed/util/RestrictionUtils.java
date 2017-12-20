package com.latticeengines.domain.exposed.util;

import static com.latticeengines.domain.exposed.query.ComparisonType.IS_NOT_NULL;
import static com.latticeengines.domain.exposed.query.ComparisonType.IS_NULL;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AggregationSelector;
import com.latticeengines.domain.exposed.query.AggregationType;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TransactionRestriction;

public class RestrictionUtils {

    public static Restriction convertBucketRestriction(BucketRestriction bucketRestriction) {
        Bucket bkt = bucketRestriction.getBkt();
        AttributeLookup attr = bucketRestriction.getAttr();
        if (bkt == null) {
            throw new IllegalArgumentException("cannot convert null bucket restriction");
        }

        if (BusinessEntity.TRANSACTION_ENTITIES.contains(attr.getEntity())) {
            Bucket.Transaction transaction = bkt.getTransaction();
            if (transaction == null) {
                throw new IllegalArgumentException("A " + attr.getEntity() + " bucket must have transaction (Txn) field.");
            } else {
                return convertTxnBucket(transaction);
            }
        }

        ComparisonType comparisonType = bkt.getComparisonType();
        List<Object> values = bkt.getValues();

        if (comparisonType == null) {
            throw new UnsupportedOperationException(
                    "Bucket without comparator is obsolete. You might need to update your query to latest schema.");
        } else {
            return RestrictionUtils.convertValueComparisons(attr, comparisonType, values);
        }
    }

    private static Restriction convertTxnBucket(Bucket.Transaction transaction) {
        TransactionRestriction transactionRestriction = new TransactionRestriction();
        transactionRestriction.setProductId(transaction.getProductId());
        transactionRestriction.setTimeFilter(transaction.getTimeFilter());
        transactionRestriction.setNegate(Boolean.TRUE.equals(transaction.getNegate()));

        AggregationFilter unitFilterInTxn = transaction.getUnitFilter();
        if (unitFilterInTxn != null) {
            AggregationType agg = AggregationType.SUM;
            if (unitFilterInTxn.getAggregationType() != null) {
                agg = unitFilterInTxn.getAggregationType();
            }
            AggregationFilter unitFilter = new AggregationFilter(AggregationSelector.UNIT, agg, //
                    unitFilterInTxn.getComparisonType(), unitFilterInTxn.getValues());
            transactionRestriction.setUnitFilter(unitFilter);
        }

        AggregationFilter spentFilterInTxn = transaction.getSpentFilter();
        if (spentFilterInTxn != null) {
            AggregationType agg = AggregationType.SUM;
            if (spentFilterInTxn.getAggregationType() != null) {
                agg = spentFilterInTxn.getAggregationType();
            }
            AggregationFilter spentFilter = new AggregationFilter(AggregationSelector.SPENT, agg, //
                    spentFilterInTxn.getComparisonType(), spentFilterInTxn.getValues());
            transactionRestriction.setSpentFilter(spentFilter);
        }
        return transactionRestriction;
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
        case NOT_EQUAL:
        case GREATER_THAN:
        case GREATER_OR_EQUAL:
        case LESS_THAN:
        case LESS_OR_EQUAL:
            validateSingleValue(values);
            restriction = convertUnitaryValueComparison(attr, comparisonType, values.get(0));
            break;
        case IN_RANGE:
        case GTE_AND_LTE:
        case GT_AND_LTE:
        case GTE_AND_LT:
        case GT_AND_LT:
            validateInRangeValues(values);
            restriction = convertBinaryValueComparison(attr, comparisonType, values.get(0), values.get(1));
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

    private static Restriction convertUnitaryValueComparison(Lookup attr, ComparisonType comparisonType, Object value) {
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

    private static Restriction convertBinaryValueComparison(Lookup attr, ComparisonType comparisonType, Object min, Object max) {
        Restriction restriction = null;
        switch (comparisonType) {
            case GTE_AND_LTE:
                restriction = Restriction.builder()
                        .and( //
                                Restriction.builder().let(attr).gte(min).build(),
                                Restriction.builder().let(attr).lte(max).build())
                        .build();
                break;
            case GT_AND_LTE:
                restriction = Restriction.builder()
                        .and( //
                                Restriction.builder().let(attr).gt(min).build(),
                                Restriction.builder().let(attr).lte(max).build())
                        .build();
                break;
            case GTE_AND_LT:
                restriction = Restriction.builder()
                        .and( //
                                Restriction.builder().let(attr).gte(min).build(),
                                Restriction.builder().let(attr).lt(max).build())
                        .build();
                break;
            case GT_AND_LT:
                restriction = Restriction.builder()
                        .and( //
                                Restriction.builder().let(attr).gt(min).build(),
                                Restriction.builder().let(attr).lt(max).build())
                        .build();
                break;
            default:
                restriction = Restriction.builder().let(attr).in(min, max).build();
                break;
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
