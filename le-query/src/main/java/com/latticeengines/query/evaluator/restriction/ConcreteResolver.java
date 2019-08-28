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
import org.apache.commons.lang3.tuple.Pair;
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
import com.latticeengines.domain.exposed.util.RestrictionUtils;
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
        ComparisonType operator = restriction.getRelation();
        boolean negate = Boolean.TRUE.equals(restriction.getNegate());

        boolean isBitEncoded = isBitEncoded(lhs, operator, rhs);
        boolean isBitEncodedNullQuery = false;

        if (isBitEncoded) {
            // process bit encoded restrictions
            AttributeLookup attrLookup = (AttributeLookup) lhs;
            if (ComparisonType.IS_NULL.equals(operator)) {
                // is null means bktId = 0
                operator = ComparisonType.EQUAL;
                rhs = new ValueLookup(0);
                isBitEncodedNullQuery = true;
            } else if (ComparisonType.IS_NOT_NULL.equals(operator)) {
                // is not null means bktId != 0
                operator = ComparisonType.NOT_EQUAL;
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
                if (operator.isLikeTypeOfComparison()) {
                    Pair<ComparisonType, Lookup> pair = //
                            convertBitEncodedValueLookup(operator, bkts, (String) valueLookup.getValue());
                    operator = pair.getLeft();
                    rhs = pair.getRight();
                } else {
                    rhs = convertBitEncodedValueLookup(bkts, (String) valueLookup.getValue());
                }
            }
        } else if (lhs instanceof AttributeLookup) {
            // if not bit encoded, need to cast the type of operands
            AttributeLookup attrLookup = (AttributeLookup) lhs;
            ColumnMetadata cm = getAttrRepo().getColumnMetadata(attrLookup);
            Class<?> javaClz = parseNumericalJavaClass(cm.getJavaClass());
            if (javaClz != null) {
                if (rhs instanceof CollectionLookup) {
                    Collection<Object> vals = ((CollectionLookup) rhs).getValues();
                    if (CollectionUtils.isNotEmpty(vals)) {
                        List<Object> newVals = RestrictionUtils.convertNumericalValues(new ArrayList<>(vals), javaClz);
                        ((CollectionLookup) rhs).setValues(newVals);
                    }
                } else if (rhs instanceof ValueLookup) {
                    Object val = ((ValueLookup) rhs).getValue();
                    List<Object> newVals = RestrictionUtils.convertNumericalValues( //
                            Collections.singletonList(val), javaClz);
                    ((ValueLookup) rhs).setValue(newVals.get(0));
                }
            }
        }

        // simplify some negative restrictions
        if (negate) {
            if (ComparisonType.NOT_EQUAL.equals(operator)) {
                log.info("Converting [Not NOT_EQUAL] to [EQUAL]");
                operator = ComparisonType.EQUAL;
                negate = false;
            }
            if (ComparisonType.NOT_CONTAINS.equals(operator)) {
                log.info("Converting [Not NOT_CONTAINS] to [CONTAINS]");
                operator = ComparisonType.CONTAINS;
                negate = false;
            }
            if (ComparisonType.NOT_IN_COLLECTION.equals(operator)) {
                log.info("Converting [Not NOT_IN_COLLECTION] to [IN_COLLECTION]");
                operator = ComparisonType.IN_COLLECTION;
                negate = false;
            }
            if (ComparisonType.IS_NOT_NULL.equals(operator)) {
                log.info("Converting [Not IS_NOT_NULL] to [IS_NULL]");
                operator = ComparisonType.IS_NULL;
                negate = false;

            }
        }

        LookupResolver lhsResolver = lookupFactory.getLookupResolver(lhs.getClass());
        List<ComparableExpression> lhsPaths = lhsResolver.resolveForCompare(lhs);
        ComparableExpression lhsPath = lhsPaths.get(0);

        if (ComparisonType.EQUAL.equals(operator) && isNullValueLookup(rhs)) {
                if (negate) {
                log.info("Converting [Not EQUAL null] to [IS_NOT_NULL]");
                operator = ComparisonType.IS_NOT_NULL;
                negate = false;
            } else {
                log.info("Converting [EQUAL null] to [IS_NULL]");
                operator = ComparisonType.IS_NULL;
            }
        }

        if (ComparisonType.IS_NULL.equals(operator)) {
            return lhsPath.isNull();
        } else if (ComparisonType.IS_NOT_NULL.equals(operator)) {
            return lhsPath.isNotNull();
        } else {
            LookupResolver rhsResolver = lookupFactory.getLookupResolver(rhs.getClass());
            List<ComparableExpression> rhsPaths = rhsResolver.resolveForCompare(rhs);

            BooleanExpression booleanExpression;

            switch (operator) {
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
                                new String[] { operator.toString() });
                    }
                case STARTS_WITH:
                    if (lhsPath instanceof StringExpression) {
                        booleanExpression = Expressions.asString(lhsPath).startsWithIgnoreCase(rhsPaths.get(0));
                        break;
                    } else {
                        throw new LedpException(LedpCode.LEDP_37006,
                                new String[] { operator.toString() });
                    }
                case ENDS_WITH:
                    if (lhsPath instanceof StringExpression) {
                        booleanExpression = Expressions.asString(lhsPath).endsWithIgnoreCase(rhsPaths.get(0));
                        break;
                    } else {
                        throw new LedpException(LedpCode.LEDP_37006,
                                new String[] { operator.toString() });
                    }
                case NOT_CONTAINS:
                default:
                    throw new LedpException(LedpCode.LEDP_37006,
                            new String[] { operator.toString() });
            }

            if (negate) {
                booleanExpression = booleanExpression.not();
            }

            if (isBitEncoded && !isBitEncodedNullQuery && isNegativeBitEncodedLookup(operator, negate)) {
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

    private boolean isBitEncoded(Lookup lhs, ComparisonType operator, Lookup rhs) {
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
                        .contains(operator)) {
                    return true;
                }
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

    // these bit encoded restrictions imply encoded attr is not null
    private boolean isNegativeBitEncodedLookup(ComparisonType operator, boolean negate) {
        boolean negativeOperator = !negate && ( //
                Arrays.asList( //
                        ComparisonType.NOT_EQUAL, //
                        ComparisonType.NOT_CONTAINS, //
                        ComparisonType.NOT_IN_COLLECTION //
                ).contains(operator));
        boolean positiveOperator = negate && ( //
                Arrays.asList( //
                        ComparisonType.EQUAL, //
                        ComparisonType.CONTAINS, //
                        ComparisonType.IN_COLLECTION //
                ).contains(operator));
        return negativeOperator || positiveOperator;
    }

    private ValueLookup convertBitEncodedValueLookup(Buckets buckets, String val) {
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

    private Pair<ComparisonType, Lookup> convertBitEncodedValueLookup(ComparisonType operator, Buckets buckets,
                                                                      String bktLbl) {
        List<Object> ids = new ArrayList<>();
        if (bktLbl != null) {
            ids = buckets.getBucketList().stream() //
                    .filter(b -> operator.filter(getBktVal(b), bktLbl)) //
                    .map(Bucket::getIdAsInt) //
                    .collect(Collectors.toList());
        }
        if (ids.isEmpty()) {
            log.warn("Cannot find corresponding label for " + operator + " " //
                    + bktLbl + " in statistics, use -1 bkt id instead.");
            return Pair.of(ComparisonType.EQUAL, new ValueLookup(-1));
        }
        if (ids.size() > 1) {
            return Pair.of(ComparisonType.IN_COLLECTION, new CollectionLookup(ids));
        } else {
            return Pair.of(ComparisonType.EQUAL, new ValueLookup(ids.get(0)));
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

    private Class<?> parseNumericalJavaClass(String javaClzName) {
        Class<?> javaClz = null;
        if (StringUtils.isNotBlank(javaClzName)) {
            try {
                javaClz = Class.forName("java.lang." + javaClzName);
            } catch (ClassNotFoundException e) {
                log.error("Cannot parse java class " + javaClzName);
            }
        }
        if (javaClz != null && Number.class.isAssignableFrom(javaClz)) {
            return javaClz;
        } else {
            return null;
        }
    }

}
