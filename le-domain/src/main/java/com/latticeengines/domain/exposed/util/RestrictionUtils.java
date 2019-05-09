package com.latticeengines.domain.exposed.util;

import static com.latticeengines.domain.exposed.query.ComparisonType.IS_EMPTY;
import static com.latticeengines.domain.exposed.query.ComparisonType.IS_NOT_NULL;
import static com.latticeengines.domain.exposed.query.ComparisonType.IS_NULL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.graph.traversal.impl.BreadthFirstSearch;
import com.latticeengines.common.exposed.graph.traversal.impl.DepthFirstSearch;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.NullMetricsImputation;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AggregationSelector;
import com.latticeengines.domain.exposed.query.AggregationType;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.DateRestriction;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.RangeLookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.RestrictionBuilder;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.TransactionRestriction;

public class RestrictionUtils {

    private static final Logger log = LoggerFactory.getLogger(RestrictionUtils.class);

    public static final AttributeLookup TRANSACTION_LOOKUP = new AttributeLookup(BusinessEntity.PurchaseHistory,
            "HasPurchased");

    public static void inspectBucketRestriction(BucketRestriction bucketRestriction,
            Map<ComparisonType, Set<AttributeLookup>> map, TimeFilterTranslator timeTranslator) {
        Bucket bkt = bucketRestriction.getBkt();
        if (bkt == null) {
            throw new IllegalArgumentException("cannot inspect null bucket restriction");
        }

        if (bkt.getDateFilter() != null) {
            ComparisonType cmp = bkt.getDateFilter().getRelation();
            AttributeLookup lookup = bucketRestriction.getAttr();
            // If the current operator needs further specification
            // and the TimeFilterTranslator does not have its specs
            if (ComparisonType.VAGUE_TYPES.contains(cmp)
                    && !timeTranslator.getSpecifiedValues().get(cmp).containsKey(lookup)) {
                if (map.containsKey(cmp)) {
                    map.get(cmp).add(lookup);
                } else {
                    Set<AttributeLookup> set = new HashSet<>();
                    set.add(lookup);
                    map.put(cmp, set);
                }

            }
        }
    }

    public static Restriction cleanupBucketsInRestriction(Restriction restriction) {
        if (restriction != null) {
            if (restriction instanceof LogicalRestriction) {
                BreadthFirstSearch search = new BreadthFirstSearch();
                search.run(restriction, (object, ctx) -> {
                    if (object instanceof BucketRestriction) {
                        cleanupBucketRestriction((BucketRestriction) object);
                    }
                });
            } else if (restriction instanceof BucketRestriction) {
                cleanupBucketRestriction((BucketRestriction) restriction);
            }
        }
        return restriction;
    }

    private static void cleanupBucketRestriction(BucketRestriction bucketRestriction) {
        Bucket bkt = bucketRestriction.getBkt();
        bkt.setId(null);
        bkt.setLabel(null);
        if (isValueFreeOperator(bkt.getComparisonType())) {
            bkt.setValues(null);
        }
        bucketRestriction.setBkt(bkt);
    }

    private static boolean isValueFreeOperator(ComparisonType operator) {
        return IS_NULL.equals(operator) || IS_NOT_NULL.equals(operator) || IS_EMPTY.equals(operator);
    }

    public static List<BucketRestriction> validateBktsInRestriction(Restriction restriction) {
        List<BucketRestriction> invalidBkts = new ArrayList<>();
        if (restriction != null) {
            if (restriction instanceof LogicalRestriction) {
                BreadthFirstSearch search = new BreadthFirstSearch();
                search.run(restriction, (object, ctx) -> {
                    accumulateInvalidBkts(object, invalidBkts);
                });
            } else if (restriction instanceof BucketRestriction) {
                accumulateInvalidBkts(restriction, invalidBkts);
            }
        }
        return invalidBkts;
    }

    private static void accumulateInvalidBkts(Object obj, List<BucketRestriction> invalidBkts) {
        if (obj instanceof BucketRestriction) {
            BucketRestriction bucket = (BucketRestriction) obj;
            if (!Boolean.TRUE.equals(bucket.getIgnored())) {
                try {
                    RestrictionUtils.validateBucket(bucket);
                } catch (Exception e) {
                    log.warn("Invalid bucket: " + JsonUtils.serialize(bucket), e);
                    invalidBkts.add(bucket);
                }
            }
        }
    }

    private static void validateBucket(BucketRestriction bucketRestriction) {
        Bucket bkt = bucketRestriction.getBkt();
        AttributeLookup attr = bucketRestriction.getAttr();
        if (BusinessEntity.PurchaseHistory.equals(attr.getEntity())) {
            // PH buckets
            return;
        }
        if (bkt.getTransaction() != null) {
            // special buckets
            return;
        }
        if (bkt.getDateFilter() != null) {
            // special buckets
            return;
        }
        if (bkt.getChange() != null) {
            // special buckets
            return;
        }
        ComparisonType comparisonType = bkt.getComparisonType();
        validateComparatorAndValues(comparisonType, bkt.getValues());
    }

    private static void validateComparatorAndValues(ComparisonType comparator, List<Object> values) {
        if (comparator == null) {
            throw new UnsupportedOperationException("Bucket without comparator is obsolete.");
        }
        switch (comparator) {
        case IS_NULL:
        case IS_NOT_NULL:
            validateEmptyValue(comparator, values);
            break;
        case EQUAL:
        case NOT_EQUAL:
        case GREATER_THAN:
        case GREATER_OR_EQUAL:
        case LESS_THAN:
        case LESS_OR_EQUAL:
        case CONTAINS:
        case NOT_CONTAINS:
        case STARTS_WITH:
        case ENDS_WITH:
            validateSingleValue(comparator, values);
            break;
        case GTE_AND_LTE:
        case GT_AND_LTE:
        case GTE_AND_LT:
        case GT_AND_LT:
        case BETWEEN:
            validateInRangeValues(values);
            break;
        case IN_COLLECTION:
        case NOT_IN_COLLECTION:
            validateNonEmptyValue(comparator, values);
            break;
        default:
            throw new UnsupportedOperationException("comparator " + comparator + " is not supported yet");
        }
    }

    public static Restriction convertBucketRestriction(BucketRestriction bucketRestriction,
            boolean translatePriorOnly) {
        Restriction restriction;
        Bucket bkt = bucketRestriction.getBkt();
        if (bkt == null) {
            throw new IllegalArgumentException("cannot convert null bucket restriction");
        }

        if (bkt.getChange() != null) {
            bkt = StatsCubeUtils.convertChgBucketToBucket(bkt);
        }

        if (bkt.getTransaction() != null) {
            restriction = convertTxnBucket(bkt.getTransaction(), translatePriorOnly);
        } else if (bkt.getDateFilter() != null) {
            restriction = convertDateBucket(bucketRestriction, translatePriorOnly);
        } else {
            ComparisonType comparisonType = bkt.getComparisonType();
            List<Object> values = bkt.getValues();
            if (comparisonType == null) {
                throw new UnsupportedOperationException(
                        "Bucket without comparator is obsolete. You might need to update your query to latest schema.");
            } else {
                AttributeLookup attr = bucketRestriction.getAttr();
                if (BusinessEntity.PurchaseHistory.equals(attr.getEntity())) {
                    restriction = convertPurchaseHistoryBucket(attr, comparisonType, values);
                } else {
                    restriction = convertValueComparisons(attr, comparisonType, values);
                }
            }
        }
        return restriction;
    }

    public static Restriction convertConcreteRestriction(ConcreteRestriction concreteRestriction) {
        if (Arrays.asList(ComparisonType.GTE_AND_LTE, ComparisonType.GT_AND_LTE, ComparisonType.GTE_AND_LT,
                ComparisonType.GT_AND_LT).contains(concreteRestriction.getRelation())) {
            RangeLookup rangeLookup = (RangeLookup) concreteRestriction.getRhs();
            return convertBinaryValueComparison( //
                    concreteRestriction.getLhs(), concreteRestriction.getRelation(), rangeLookup.getMin(),
                    rangeLookup.getMax());
        } else {
            return concreteRestriction;
        }
    }

    private static Restriction convertDateBucket(BucketRestriction bucketRestriction, boolean translatePriorOnly) {
        if (bucketRestriction.getBkt() != null && bucketRestriction.getBkt().getDateFilter() != null
                && translatePriorOnly
                && ComparisonType.PRIOR_ONLY.equals(bucketRestriction.getBkt().getDateFilter().getRelation())) {
            throw new UnsupportedOperationException("Does not support PRIOR_ONLY operation");
        } else {
            DateRestriction dateRestriction = new DateRestriction();
            dateRestriction.setAttr(bucketRestriction.getAttr());
            dateRestriction.setTimeFilter(bucketRestriction.getBkt().getDateFilter());
            return dateRestriction;
        }
    }

    private static Restriction convertTxnBucket(Bucket.Transaction transaction, boolean translatePriorOnly) {
        if (transaction.getTimeFilter() != null && translatePriorOnly
                && ComparisonType.PRIOR_ONLY.equals(transaction.getTimeFilter().getRelation())) {
            return convertPriorOnlyTxnBucket(transaction);
        } else {
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
                        unitFilterInTxn.getComparisonType(), unitFilterInTxn.getValues(),
                        unitFilterInTxn.isIncludeNotPurchased());
                transactionRestriction.setUnitFilter(unitFilter);
            }

            AggregationFilter spentFilterInTxn = transaction.getSpentFilter();
            if (spentFilterInTxn != null) {
                AggregationType agg = AggregationType.SUM;
                if (spentFilterInTxn.getAggregationType() != null) {
                    agg = spentFilterInTxn.getAggregationType();
                }
                AggregationFilter spentFilter = new AggregationFilter(AggregationSelector.SPENT, agg, //
                        spentFilterInTxn.getComparisonType(), spentFilterInTxn.getValues(),
                        spentFilterInTxn.isIncludeNotPurchased());
                transactionRestriction.setSpentFilter(spentFilter);
            }
            return transactionRestriction;
        }
    }

    private static Restriction convertPriorOnlyTxnBucket(Bucket.Transaction transaction) {
        boolean negate = Boolean.TRUE.equals(transaction.getNegate());
        String period = transaction.getTimeFilter().getPeriod();
        if (transaction.getTimeFilter().getValues().size() != 1) {
            throw new RuntimeException("Prior only time filter should only have one value, but found "
                    + transaction.getTimeFilter().getValues());
        }
        int val = Integer.valueOf(String.valueOf(transaction.getTimeFilter().getValues().get(0)));
        TimeFilter prior = TimeFilter.prior(val, period);
        TimeFilter within = TimeFilter.withinInclude(val, period);
        Bucket.Transaction priorTxn, withinTxn;
        if (negate) {
            priorTxn = new Bucket.Transaction(transaction.getProductId(), prior, transaction.getSpentFilter(),
                    transaction.getUnitFilter(), true);
            withinTxn = new Bucket.Transaction(transaction.getProductId(), within, transaction.getSpentFilter(),
                    transaction.getUnitFilter(), false);
        } else {
            priorTxn = new Bucket.Transaction(transaction.getProductId(), prior, transaction.getSpentFilter(),
                    transaction.getUnitFilter(), false);
            withinTxn = new Bucket.Transaction(transaction.getProductId(), within, transaction.getSpentFilter(),
                    transaction.getUnitFilter(), true);
        }
        Restriction everRst = convertTxnBucket(priorTxn, false);
        Restriction withinRst = convertTxnBucket(withinTxn, false);
        RestrictionBuilder builder = Restriction.builder();
        if (negate) {
            builder.or(everRst, withinRst);
        } else {
            builder.and(everRst, withinRst);
        }
        return builder.build();
    }

    private static Restriction convertPurchaseHistoryBucket(AttributeLookup attr, ComparisonType comparator,
            List<Object> values) {
        String fullAttrName = attr.getAttribute();
        String metricAttr = ActivityMetricsUtils.getNameWithPeriodFromFullName(fullAttrName);
        String bundleId = ActivityMetricsUtils.getProductIdFromFullName(fullAttrName);
        AttributeLookup metricAttrLookup = new AttributeLookup(BusinessEntity.DepivotedPurchaseHistory, metricAttr);
        Restriction metricRestriction = convertValueComparisons(metricAttrLookup, comparator, values);
        Restriction bundleIdRestriction = Restriction.builder() //
                .let(BusinessEntity.DepivotedPurchaseHistory, InterfaceName.ProductId.name()) //
                .eq(bundleId).build();
        Restriction restriction = Restriction.builder().and(bundleIdRestriction, metricRestriction).build();

        NullMetricsImputation imputation = ActivityMetricsUtils.getNullImputation(fullAttrName);
        // only handles zero imputation now
        boolean needImputeNulls = NullMetricsImputation.ZERO.equals(imputation) && containsZero(comparator, values);
        if (needImputeNulls) {
            Restriction isNullRestriction = Restriction.builder() //
                    .let(BusinessEntity.DepivotedPurchaseHistory, InterfaceName.ProductId.name()).isNull().build();
            restriction = Restriction.builder().or(restriction, isNullRestriction).build();
        }

        return restriction;
    }

    private static Restriction convertValueComparisons(Lookup attr, ComparisonType comparisonType,
            List<Object> values) {
        Restriction restriction = null;
        switch (comparisonType) {
        case IS_NULL:
            validateEmptyValue(comparisonType, values);
            restriction = new ConcreteRestriction(false, attr, IS_NULL, null);
            break;
        case IS_NOT_NULL:
            validateEmptyValue(comparisonType, values);
            restriction = new ConcreteRestriction(false, attr, IS_NOT_NULL, null);
            break;
        case EQUAL:
        case NOT_EQUAL:
        case GREATER_THAN:
        case GREATER_OR_EQUAL:
        case LESS_THAN:
        case LESS_OR_EQUAL:
            validateSingleValue(comparisonType, values);
            restriction = convertUnitaryValueComparison(attr, comparisonType, values.get(0));
            break;
        case GTE_AND_LTE:
        case GT_AND_LTE:
        case GTE_AND_LT:
        case GT_AND_LT:
            validateInRangeValues(values);
            restriction = convertBinaryValueComparison(attr, comparisonType, values.get(0), values.get(1));
            break;
        case IN_COLLECTION:
            validateNonEmptyValue(comparisonType, values);
            restriction = Restriction.builder().let(attr).inCollection(values).build();
            break;
        case NOT_IN_COLLECTION:
            validateNonEmptyValue(comparisonType, values);
            restriction = Restriction.builder().let(attr).notInCollection(values).build();
            break;
        case CONTAINS:
            validateSingleValue(comparisonType, values);
            restriction = Restriction.builder().let(attr).contains(values.get(0)).build();
            break;
        case NOT_CONTAINS:
            validateSingleValue(comparisonType, values);
            restriction = Restriction.builder().let(attr).notcontains(values.get(0)).build();
            break;
        case STARTS_WITH:
            validateSingleValue(comparisonType, values);
            restriction = Restriction.builder().let(attr).not().startsWith(values.get(0)).build();
            break;
        case ENDS_WITH:
            validateSingleValue(comparisonType, values);
            restriction = Restriction.builder().let(attr).not().endsWith(values.get(0)).build();
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

    private static Restriction convertBinaryValueComparison(Lookup attr, ComparisonType comparisonType, Object min,
            Object max) {
        Restriction restriction;
        switch (comparisonType) {
        case GTE_AND_LTE:
            restriction = Restriction.builder().and( //
                    Restriction.builder().let(attr).gte(min).build(), Restriction.builder().let(attr).lte(max).build())
                    .build();
            break;
        case GT_AND_LTE:
            restriction = Restriction.builder().and( //
                    Restriction.builder().let(attr).gt(min).build(), Restriction.builder().let(attr).lte(max).build())
                    .build();
            break;
        case GTE_AND_LT:
            restriction = Restriction.builder().and( //
                    Restriction.builder().let(attr).gte(min).build(), Restriction.builder().let(attr).lt(max).build())
                    .build();
            break;
        case GT_AND_LT:
            restriction = Restriction.builder().and( //
                    Restriction.builder().let(attr).gt(min).build(), Restriction.builder().let(attr).lt(max).build())
                    .build();
            break;
        default:
            throw new UnsupportedOperationException("Unknown operator " + comparisonType);
        }
        return restriction;
    }

    private static void validateSingleValue(ComparisonType comparisonType, List<Object> values) {
        if (values == null || values.size() != 1) {
            throw new IllegalArgumentException(comparisonType + " should have exactly one value.");
        }
    }

    private static void validateInRangeValues(List<Object> values) {
        if (values == null || values.size() != 2) {
            throw new IllegalArgumentException("range should contain both min and max value.");
        }
    }

    private static void validateEmptyValue(ComparisonType comparisonType, List<Object> values) {
        if (CollectionUtils.isNotEmpty(values)) {
            throw new IllegalArgumentException(comparisonType + " should not have any value.");
        }
    }

    private static void validateNonEmptyValue(ComparisonType comparisonType, List<Object> values) {
        if (CollectionUtils.isEmpty(values)) {
            throw new IllegalArgumentException(comparisonType + " should have at least one value.");
        }
    }

    public static Set<AttributeLookup> getRestrictionDependingAttributes(Restriction restriction) {
        Set<AttributeLookup> attributes = new HashSet<>();
        DepthFirstSearch search = new DepthFirstSearch();
        search.run(restriction, (object, ctx) -> {
            GraphNode node = (GraphNode) object;
            if (node instanceof AttributeLookup) {
                attributes.add(((AttributeLookup) node));
            }
        });

        return attributes;
    }

    private static boolean containsZero(ComparisonType comparator, List<Object> vals) {
        boolean containsZero = false;
        if (vals.size() == 2) {
            Double upperBound = toDouble(vals.get(1));
            Double lowerBound = toDouble(vals.get(0));
            containsZero = lowerBound * upperBound <= 0;
        } else {
            Double val = toDouble(vals.get(0));
            switch (comparator) {
            case GREATER_OR_EQUAL:
                containsZero = val <= 0;
                break;
            case GREATER_THAN:
                containsZero = val < 0;
                break;
            case LESS_OR_EQUAL:
                containsZero = val >= 0;
                break;
            case LESS_THAN:
                containsZero = val > 0;
                break;
            default:
                break;
            }
        }
        return containsZero;
    }

    private static Double toDouble(Object val) {
        if (val == null) {
            return null;
        } else if (val instanceof Double) {
            return (Double) val;
        } else {
            return Double.valueOf(val.toString());
        }
    }
}
