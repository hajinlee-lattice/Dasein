package com.latticeengines.query.evaluator.restriction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.CaseLookup;
import com.latticeengines.domain.exposed.query.CollectionLookup;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
import com.latticeengines.domain.exposed.query.ValueLookup;
import com.latticeengines.query.evaluator.lookup.LookupResolver;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.StringExpression;
import com.querydsl.sql.SQLQuery;

public class ConcreteResolver extends BaseRestrictionResolver<ConcreteRestriction>
        implements RestrictionResolver<ConcreteRestriction> {

    private static final Logger log = LoggerFactory.getLogger(ConcreteResolver.class);

    ConcreteResolver(RestrictionResolverFactory factory) {
        super(factory);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public BooleanExpression resolve(ConcreteRestriction restriction) {
        Lookup lhs = restriction.getLhs();
        Lookup rhs = restriction.getRhs();

        boolean isBitEncoded = isBitEncoded(restriction);
        boolean isBitEncodedNullQuery = false;

        if (isBitEncoded) {
            AttributeLookup attrLookup = (AttributeLookup) lhs;
            if (ComparisonType.IS_NULL.equals(restriction.getRelation())) {
                // is null means bktId = 0
                restriction.setRelation(ComparisonType.EQUAL);
                rhs = new ValueLookup(0);
                isBitEncodedNullQuery = true;
            } else if (ComparisonType.IS_NOT_NULL.equals(restriction.getRelation())) {
                // is not null means bktId != 0
                restriction.setRelation(ComparisonType.NOT_EQUAL);
                rhs = new ValueLookup(0);
                isBitEncodedNullQuery = true;
            } else if (rhs instanceof CollectionLookup) {
                CollectionLookup collectionLookup = (CollectionLookup) rhs;
                AttributeStats stats = findAttributeStats(attrLookup);
                Buckets bkts = stats.getBuckets();
                rhs = convertCollectionLookup(bkts, collectionLookup.getValues());
            } else {
                ValueLookup valueLookup = (ValueLookup) rhs;
                AttributeStats stats = findAttributeStats(attrLookup);
                Buckets bkts = stats.getBuckets();
                if (restriction.getRelation().isLikeTypeOfComparison()) {
                    rhs = convertValueLookup(restriction, bkts, (String) valueLookup.getValue());
                } else {
                    rhs = convertValueLookup(bkts, (String) valueLookup.getValue());
                }
            }
        }

        if (Boolean.TRUE.equals(restriction.getNegate())) {
            if (restriction.getRelation().equals(ComparisonType.NOT_EQUAL)) {
                log.info("Converting [Not NOT_EQUAL] to [EQUAL]");
                restriction.setRelation(ComparisonType.EQUAL);
                restriction.setNegate(false);
            }
            if (restriction.getRelation().equals(ComparisonType.NOT_CONTAINS)) {
                log.info("Converting [Not NOT_CONTAINS] to [CONTAINS]");
                restriction.setRelation(ComparisonType.CONTAINS);
                restriction.setNegate(false);
            }
            if (restriction.getRelation().equals(ComparisonType.NOT_IN_COLLECTION)) {
                log.info("Converting [Not NOT_IN_COLLECTION] to [IN_COLLECTION]");
                restriction.setRelation(ComparisonType.IN_COLLECTION);
                restriction.setNegate(false);
            }
            if (restriction.getRelation().equals(ComparisonType.IS_NOT_NULL)) {
                log.info("Converting [Not IS_NOT_NULL] to [IS_NULL]");
                restriction.setRelation(ComparisonType.IS_NULL);
                restriction.setNegate(false);
            }
        }

        LookupResolver lhsResolver = lookupFactory.getLookupResolver(lhs.getClass());
        List<ComparableExpression> lhsPaths = lhsResolver.resolveForCompare(lhs);
        ComparableExpression lhsPath = lhsPaths.get(0);

        if (restriction.getRelation().equals(ComparisonType.EQUAL) && isNullValueLookup(rhs)) {
            if (restriction.getNegate()) {
                restriction.setRelation(ComparisonType.IS_NOT_NULL);
                restriction.setNegate(false);
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
            List<ComparableExpression> rhsPaths = rhsResolver.resolveForCompare(rhs);

            BooleanExpression booleanExpression;

            switch (restriction.getRelation()) {
                case EQUAL:
                    if (rhs instanceof SubQueryAttrLookup) {
                        SubQueryAttrLookup subQueryAttrLookup = (SubQueryAttrLookup) rhs;
                        if (StringUtils.isBlank(subQueryAttrLookup.getAttribute())) {
                            booleanExpression = lhsPaths.get(0).eq((SQLQuery<?>) subQueryAttrLookup
                                    .getSubQuery().getSubQueryExpression());
                        } else {
                            ComparableExpression<String> subselect = rhsResolver
                                    .resolveForSubselect(rhs);
                            booleanExpression = lhsPath.eq(subselect);
                        }
                    } else {
                        if (applyEqualIgnoreCase(isBitEncoded, lhs, rhs, lhsPath)) {
                            booleanExpression = Expressions.asString(lhsPath).equalsIgnoreCase(rhsPaths.get(0));
                        } else {
                            booleanExpression = lhsPath.eq(rhsPaths.get(0));
                        }
                    }
                    break;
                case NOT_EQUAL:
                    if (rhs instanceof SubQueryAttrLookup) {
                        SubQueryAttrLookup subQueryAttrLookup = (SubQueryAttrLookup) rhs;
                        if (StringUtils.isBlank(subQueryAttrLookup.getAttribute())) {
                            booleanExpression = lhsPaths.get(0).ne((SQLQuery<?>) subQueryAttrLookup
                                    .getSubQuery().getSubQueryExpression());
                        } else {
                            ComparableExpression<String> subselect = rhsResolver
                                    .resolveForSubselect(rhs);
                            booleanExpression = lhsPath.ne(subselect);
                        }
                    } else {
                        if (applyEqualIgnoreCase(isBitEncoded, lhs, rhs, lhsPath)) {
                            booleanExpression = Expressions.asString(lhsPath).notEqualsIgnoreCase(rhsPaths.get(0));
                        } else {
                            booleanExpression = lhsPath.ne(rhsPaths.get(0));
                        }
                    }
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
                case NOT_IN_COLLECTION:
                    if (rhs instanceof SubQueryAttrLookup) {
                        SubQueryAttrLookup subQueryAttrLookup = (SubQueryAttrLookup) rhs;
                        if (StringUtils.isBlank(subQueryAttrLookup.getAttribute())) {
                            booleanExpression = lhsPaths.get(0)
                                    .notIn((SQLQuery<?>) subQueryAttrLookup.getSubQuery()
                                            .getSubQueryExpression());
                        } else {
                            ComparableExpression<String> subselect = rhsResolver
                                    .resolveForSubselect(rhs);
                            booleanExpression = lhsPaths.get(0).notIn(subselect);
                        }
                    } else {
                        if (rhsPaths.size() > 1) {
                            if (applyEqualIgnoreCase(isBitEncoded, lhs, rhs, lhsPath)) {
                                rhsPaths = rhsResolver.resolveForLowercaseCompare(rhs);
                                booleanExpression = Expressions.asString(lhsPath).toLowerCase()
                                        .notIn(rhsPaths.toArray(new ComparableExpression[0]));

                            } else {
                                booleanExpression = lhsPath
                                        .notIn(rhsPaths.toArray(new ComparableExpression[0]));
                            }
                        } else {
                            if (applyEqualIgnoreCase(isBitEncoded, lhs, rhs, lhsPath)) {
                                booleanExpression = Expressions.asString(lhsPath).notEqualsIgnoreCase(rhsPaths.get(0));
                            } else {
                                booleanExpression = lhsPath.ne(rhsPaths.get(0));
                            }
                        }
                    }
                    break;
                case IN_COLLECTION:
                    if (rhs instanceof SubQueryAttrLookup) {
                        SubQueryAttrLookup subQueryAttrLookup = (SubQueryAttrLookup) rhs;
                        if (StringUtils.isBlank(subQueryAttrLookup.getAttribute())) {
                            booleanExpression = lhsPaths.get(0).in(rhsResolver.resolveForFrom(rhs));
                        } else {
                            ComparableExpression<String> subselect = rhsResolver
                                    .resolveForSubselect(rhs);
                            booleanExpression = lhsPaths.get(0).in(subselect);
                        }
                    } else {
                        // when there's only 1 element in the collection,
                        // querydsl
                        // generates something
                        // like "attr in ?", which is not a valid syntax so we
                        // treat
                        // it differently
                        if (rhsPaths.size() > 1) {
                            if (applyEqualIgnoreCase(isBitEncoded, lhs, rhs, lhsPath)) {
                                rhsPaths = rhsResolver.resolveForLowercaseCompare(rhs);
                                booleanExpression = Expressions.asString(lhsPath).toLowerCase()
                                        .in(rhsPaths.toArray(new ComparableExpression[0]));

                            } else {
                                booleanExpression = lhsPath
                                        .in(rhsPaths.toArray(new ComparableExpression[0]));
                            }
                        } else {
                            if (applyEqualIgnoreCase(isBitEncoded, lhs, rhs, lhsPath)) {
                                booleanExpression = Expressions.asString(lhsPath).equalsIgnoreCase(rhsPaths.get(0));
                            } else {
                                booleanExpression = lhsPath.eq(rhsPaths.get(0));
                            }
                        }
                    }
                    break;
                case CONTAINS:
                    if (lhsPath instanceof StringExpression) {
                        booleanExpression = Expressions.asString(lhsPath).containsIgnoreCase(rhsPaths.get(0));
                        break;
                    } else {
                        throw new LedpException(LedpCode.LEDP_37006,
                                new String[] { restriction.getRelation().toString() });
                    }
                case STARTS_WITH:
                    if (lhsPath instanceof StringExpression) {
                        booleanExpression = Expressions.asString(lhsPath).startsWithIgnoreCase(rhsPaths.get(0));
                        break;
                    } else {
                        throw new LedpException(LedpCode.LEDP_37006,
                                new String[] { restriction.getRelation().toString() });
                    }
                case ENDS_WITH:
                    if (lhsPath instanceof StringExpression) {
                        booleanExpression = Expressions.asString(lhsPath).endsWithIgnoreCase(rhsPaths.get(0));
                        break;
                    } else {
                        throw new LedpException(LedpCode.LEDP_37006,
                                new String[] { restriction.getRelation().toString() });
                    }
                case NOT_CONTAINS:
                default:
                    throw new LedpException(LedpCode.LEDP_37006,
                            new String[] { restriction.getRelation().toString() });
            }

            if (restriction.getNegate()) {
                booleanExpression = booleanExpression.not();
            }

            if (isBitEncoded && !isBitEncodedNullQuery && isNegativeBitEncodedLookup(restriction)) {
                // for bit encoded, make sure not equal or not in collection
                // won't return null
                Restriction notNull = Restriction.builder().let(lhs).isNotNull().build();
                BooleanExpression notNullExpn = resolve((ConcreteRestriction) notNull);
                booleanExpression = booleanExpression.and(notNullExpn);
            }

            return booleanExpression;
        }
    }

    private boolean isNotStringValue(Lookup rhs) {
        boolean isNotStringVal = false;
        if (rhs instanceof ValueLookup) {
            ValueLookup valueLookup = (ValueLookup) rhs;
            isNotStringVal = valueLookup.getValue() != null && !(valueLookup.getValue() instanceof String);
        } else if (rhs instanceof CollectionLookup) {
            CollectionLookup collectionLookup = (CollectionLookup) rhs;
            Collection<Object> objs = collectionLookup.getValues();
            if (CollectionUtils.isNotEmpty(objs)) {
                for (Object val: objs) {
                    if (val != null && !(val instanceof String || val instanceof Character)) {
                        isNotStringVal = true;
                        break;
                    }
                }
            }
        }
        return isNotStringVal;
    }

    private boolean applyEqualIgnoreCase(boolean isBitEncoded, Lookup lhs, Lookup rhs, ComparableExpression<?> lhsPath) {
        boolean isNotStringVal = isNotStringValue(rhs);
        return !isBitEncoded && !(lhs instanceof CaseLookup) && (lhsPath instanceof StringExpression) && !isNotStringVal;
    }

    private boolean isNullValueLookup(Lookup lookup) {
        return lookup instanceof ValueLookup && ((ValueLookup) lookup).getValue() == null;
    }

    private boolean isBitEncoded(ConcreteRestriction restriction) {
        Lookup lhs = restriction.getLhs();
        if (lhs instanceof AttributeLookup) {
            AttributeLookup attrLookup = (AttributeLookup) lhs;
            if (attrLookup.getEntity() == null) {
                return false;
            }
            ColumnMetadata cm = findAttributeMetadata(attrLookup);
            if (cm == null) {
                throw new IllegalArgumentException(
                        "Cannot find metadata for attribute " + attrLookup + " in attr repo.");
            }
            if (cm.getBitOffset() != null) {
                // lhs is bit encoded
                if (Arrays.asList(ComparisonType.IS_NULL, ComparisonType.IS_NOT_NULL)
                        .contains(restriction.getRelation())) {
                    return true;
                }
                Lookup rhs = restriction.getRhs();
                if (rhs instanceof ValueLookup) {
                    ValueLookup valueLookup = (ValueLookup) rhs;
                    Object val = valueLookup.getValue();
                    if (val == null || (val instanceof String)) {
                        return true;
                    } else {
                        throw new UnsupportedOperationException(
                                "Bucket attribute can only do string value lookup. But found " + val
                                        + " instead.");
                    }
                } else if (rhs instanceof CollectionLookup) {
                    CollectionLookup colLookup = (CollectionLookup) rhs;
                    for (Object val : colLookup.getValues()) {
                        if (val != null && !(val instanceof String)) {
                            throw new UnsupportedOperationException(
                                    "Bucket attribute can only do string value lookup. But found "
                                            + val + " instead.");
                        }
                    }
                    return true;
                } else {
                    throw new UnsupportedOperationException(
                            "Bucket attribute can only do string value or collection lookup. But found "
                                    + (rhs == null ? "null" : rhs.getClass()) + " instead.");
                }
            }
        }
        return false;
    }

    private boolean isNegativeBitEncodedLookup(ConcreteRestriction restriction) {
        boolean negativeOperator = !Boolean.TRUE.equals(restriction.getNegate()) && ( //
        Arrays.asList( //
                ComparisonType.NOT_EQUAL, //
                ComparisonType.NOT_CONTAINS, //
                ComparisonType.NOT_IN_COLLECTION //
        ).contains(restriction.getRelation()));
        boolean positiveOperator = Boolean.TRUE.equals(restriction.getNegate()) && ( //
        Arrays.asList( //
                ComparisonType.EQUAL, //
                ComparisonType.CONTAINS, //
                ComparisonType.IN_COLLECTION //
        ).contains(restriction.getRelation()));
        return negativeOperator || positiveOperator;
    }

    private ValueLookup convertValueLookup(Buckets buckets, String val) {
        int value = 0;
        if (val != null) {
            Bucket bkt = buckets.getBucketList().stream() //
                    .filter(b -> getBktVal(b).equals(val)) //
                    .findFirst() //
                    .orElse(null);
            if (bkt == null) {
                log.warn("Cannot find label [" + val + "] in statistics, use dummy bkt instead.");
                value = -1;
            } else {
                value = bkt.getIdAsInt();
            }
        }
        return new ValueLookup(value);
    }

    private Lookup convertValueLookup(ConcreteRestriction restriction, Buckets buckets,
                                    String bktLbl) {
        List<Object> ids = new ArrayList<>();
        if (bktLbl != null) {
            ids = buckets.getBucketList().stream() //
                    .filter(b -> restriction.getRelation().filter(getBktVal(b), bktLbl)) //
                    .map(Bucket::getIdAsInt) //
                    .collect(Collectors.toList());
        }
        if (ids.isEmpty()) {
            log.warn("Cannot find corresponding label for " + restriction.getRelation() + " " //
                    + bktLbl + " in statistics, use -1 bkt id instead.");
            restriction.setRelation(ComparisonType.EQUAL);
            return new ValueLookup(-1);
        }
        if (ids.size() > 1) {
            restriction.setRelation(ComparisonType.IN_COLLECTION);
            return new CollectionLookup(ids);
        } else {
            restriction.setRelation(ComparisonType.EQUAL);
            return new ValueLookup(ids.get(0));
        }
    }

    private CollectionLookup convertCollectionLookup(Buckets buckets, Collection<Object> vals) {
        Set<Integer> ids = new HashSet<>();
        Set<String> valsLowerCase = vals.stream() //
                .map(val -> String.valueOf(val).toLowerCase()).collect(Collectors.toSet());
        if (CollectionUtils.isNotEmpty(vals)) {
            ids = buckets.getBucketList().stream() //
                    .filter(b -> valsLowerCase.contains(getBktVal(b).toLowerCase())) //
                    .map(Bucket::getIdAsInt) //
                    .collect(Collectors.toSet());
        }
        List<Integer> idList = new ArrayList<>(ids);
        if (idList.isEmpty()) {
            idList = new ArrayList<>(Collections.singleton(-1));
        }
        Collections.sort(idList);
        return new CollectionLookup(new ArrayList<>(idList));
    }

    private String getBktVal(Bucket bucket) {
        if (ComparisonType.EQUAL.equals(bucket.getComparisonType()) && bucket.getValues() != null
                && bucket.getValues().size() == 1) {
            return (String) bucket.getValues().get(0);
        } else {
            return bucket.getLabel();
        }
    }

}
