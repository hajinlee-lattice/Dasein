package com.latticeengines.objectapi.util;

import static com.latticeengines.query.factory.SparkQueryProvider.SPARK_BATCH_USER;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.graph.traversal.impl.BreadthFirstSearch;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AggregationType;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.CollectionLookup;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.DateRestriction;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.QueryBuilder;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.Sort;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
import com.latticeengines.domain.exposed.query.TempListUtils;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.domain.exposed.query.ValueLookup;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.domain.exposed.util.RestrictionOptimizer;
import com.latticeengines.domain.exposed.util.RestrictionUtils;
import com.latticeengines.domain.exposed.util.TimeFilterTranslator;
import com.latticeengines.objectapi.service.TempListService;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.latticeengines.query.exposed.translator.DateRangeTranslator;
import com.latticeengines.query.exposed.translator.DayRangeTranslator;
import com.latticeengines.query.exposed.translator.MetricTranslator;
import com.latticeengines.query.util.AttrRepoUtils;
import com.querydsl.core.types.EntityPath;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.sql.SQLQuery;

abstract class QueryTranslator {

    private static final Logger log = LoggerFactory.getLogger(QueryTranslator.class);

    private static final int MAX_CARDINALITY = 20000;
    // Collection lookup bigger than this will become a temp table
    private static final int MAX_IN_LINE_COLLECTION_SIZE = 200;

    final QueryFactory queryFactory;
    final AttributeRepository repository;
    final String sqlUser;
    final TimeFilterTranslator timeTranslator;
    final TempListService tempListService;

    QueryTranslator(QueryFactory queryFactory, AttributeRepository repository, String sqlUser,
            TimeFilterTranslator timeTranslator, TempListService tempListService) {
        this.queryFactory = queryFactory;
        this.repository = repository;
        this.sqlUser = sqlUser;
        this.timeTranslator = timeTranslator;
        this.tempListService = tempListService;
    }

    Restriction translateEntityQueryRestriction(FrontEndQuery frontEndQuery) {
        Restriction restriction;
        BusinessEntity mainEntity = frontEndQuery.getMainEntity();
        restriction = translateFrontEndRestriction(getEntityFrontEndRestriction(mainEntity, frontEndQuery), true);
        restriction = translateSalesforceIdRestriction(frontEndQuery, mainEntity, restriction);
        restriction = translateInnerRestriction(frontEndQuery, mainEntity, restriction);
        return restriction;
    }

    public Map<ComparisonType, Set<AttributeLookup>> needPreprocess(FrontEndQuery frontEndQuery) {
        Map<ComparisonType, Set<AttributeLookup>> results = new HashMap<>();
        BusinessEntity mainEntity = frontEndQuery.getMainEntity();

        if (BusinessEntity.Product.equals(mainEntity)) {
            return results;
        }
        needPreprocess(frontEndQuery, results);
        results.forEach((k, v) -> log.info(k + ":" + v));
        return results;
    }

    private void needPreprocess(FrontEndQuery frontEndQuery, Map<ComparisonType, Set<AttributeLookup>> map) {
        BusinessEntity mainEntity = frontEndQuery.getMainEntity();
        inspectFrontEndRestriction(getEntityFrontEndRestriction(mainEntity, frontEndQuery), map);
        inspectInnerRestriction(frontEndQuery, mainEntity, map);
    }

    Query translateProductQuery(FrontEndQuery frontEndQuery, boolean isCountQuery) {
        QueryBuilder queryBuilder = Query.builder();
        BusinessEntity mainEntity = BusinessEntity.Product;

        frontEndQuery.setAccountRestriction(null);
        frontEndQuery.setContactRestriction(null);
        frontEndQuery.setRatingModels(null);
        Restriction restriction = Restriction.builder().and(Collections.emptyList()).build();

        queryBuilder.from(mainEntity).where(restriction) //
                .orderBy(translateFrontEndSort(frontEndQuery.getSort())) //
                .page(frontEndQuery.getPageFilter()) //
                .distinct(frontEndQuery.getDistinct());

        if (isCountQuery) {
            queryBuilder.select(new ValueLookup(1));
        } else {
            queryBuilder.select(BusinessEntity.Product, InterfaceName.ProductId.name());
            queryBuilder.select(BusinessEntity.Product, InterfaceName.ProductName.name());
        }

        return queryBuilder.build();
    }

    FrontEndRestriction getEntityFrontEndRestriction(BusinessEntity entity, FrontEndQuery frontEndQuery) {
        switch (entity) {
        case Account:
            return frontEndQuery.getAccountRestriction();
        case Contact:
            return frontEndQuery.getContactRestriction();
        default:
            return null;
        }
    }

    Restriction translateFrontEndRestriction(FrontEndRestriction frontEndRestriction, boolean translatePriorOnly) {
        boolean useDepivotedPhTable = !isSparkQuery();
        Restriction restriction = translateFrontEndRestriction(frontEndRestriction, translatePriorOnly,
                useDepivotedPhTable);
        if (restriction == null) {
            return null;
        }

        Restriction translated = specifyRestrictionParameters(restriction);
        return RestrictionOptimizer.optimize(translated);
    }

    Restriction translateFrontEndRestriction(FrontEndRestriction frontEndRestriction, boolean translatePriorOnly,
            boolean useDepivotedPhTable) {
        if (frontEndRestriction == null || frontEndRestriction.getRestriction() == null) {
            return null;
        }
        Restriction restriction = translateBucketRestriction(frontEndRestriction.getRestriction().getDeepCopy(),
                translatePriorOnly, useDepivotedPhTable);
        translateBigListRestrictions(restriction);
        return RestrictionOptimizer.optimize(restriction);
    }

    private void inspectFrontEndRestriction(FrontEndRestriction frontEndRestriction,
            Map<ComparisonType, Set<AttributeLookup>> map) {
        if (frontEndRestriction == null || frontEndRestriction.getRestriction() == null) {
            return;
        }
        inspectBucketRestriction(frontEndRestriction.getRestriction(), map);
    }

    private void inspectBucketRestriction(Restriction restriction, Map<ComparisonType, Set<AttributeLookup>> map) {
        if (restriction instanceof LogicalRestriction) {
            BreadthFirstSearch search = new BreadthFirstSearch();
            search.run(restriction, (object, ctx) -> {
                if (object instanceof BucketRestriction) {
                    BucketRestriction bucket = (BucketRestriction) object;
                    if (!Boolean.TRUE.equals(bucket.getIgnored())) {
                        RestrictionUtils.inspectBucketRestriction(bucket, map, timeTranslator);
                    }
                }
            });
        } else if (restriction instanceof BucketRestriction) {
            BucketRestriction bucket = (BucketRestriction) restriction;
            if (!Boolean.TRUE.equals(bucket.getIgnored())) {
                RestrictionUtils.inspectBucketRestriction(bucket, map, timeTranslator);
            }
        }
    }

    Restriction translateSalesforceIdRestriction(FrontEndQuery frontEndQuery, BusinessEntity entity,
            Restriction restriction) {
        // only add salesforce id restriction for account entity now
        if (BusinessEntity.Account == entity) {
            if (frontEndQuery.restrictNullSalesforceId()) {
                Restriction sfidRestriction = Restriction.builder().let(entity, "SalesforceAccountID").isNull().build();
                restriction = Restriction.builder().and(restriction, sfidRestriction).build();
            } else if (frontEndQuery.restrictNotNullSalesforceId()) {
                Restriction sfidRestriction = Restriction.builder().let(entity, "SalesforceAccountID").isNotNull()
                        .build();
                restriction = Restriction.builder().and(restriction, sfidRestriction).build();
            }
        }
        return restriction;
    }

    private Restriction translateInnerRestriction(FrontEndQuery frontEndQuery, BusinessEntity outerEntity,
            Restriction outerRestriction) {
        BusinessEntity innerEntity = null;
        switch (outerEntity) {
        case Contact:
            innerEntity = BusinessEntity.Account;
            break;
        case Account:
            innerEntity = BusinessEntity.Contact;
            break;
        default:
            break;
        }
        FrontEndRestriction innerFrontEndRestriction = getEntityFrontEndRestriction(innerEntity, frontEndQuery);
        Restriction innerRestriction = translateFrontEndRestriction(innerFrontEndRestriction, true);
        return addSubselectRestriction(outerEntity, outerRestriction, innerEntity, innerRestriction);
    }

    private void inspectInnerRestriction(FrontEndQuery frontEndQuery, BusinessEntity outerEntity,
            Map<ComparisonType, Set<AttributeLookup>> map) {
        BusinessEntity innerEntity = null;
        switch (outerEntity) {
        case Contact:
            innerEntity = BusinessEntity.Account;
            break;
        case Account:
            innerEntity = BusinessEntity.Contact;
            break;
        default:
            break;
        }
        FrontEndRestriction innerFrontEndRestriction = getEntityFrontEndRestriction(innerEntity, frontEndQuery);
        inspectFrontEndRestriction(innerFrontEndRestriction, map);
    }

    Restriction translateInnerRestriction(FrontEndQuery frontEndQuery, BusinessEntity outerEntity,
            Restriction outerRestriction, boolean useDepivotedPhTable) {
        BusinessEntity innerEntity = null;
        switch (outerEntity) {
        case Contact:
            innerEntity = BusinessEntity.Account;
            break;
        case Account:
            innerEntity = BusinessEntity.Contact;
            break;
        default:
            break;
        }
        FrontEndRestriction innerFrontEndRestriction = getEntityFrontEndRestriction(innerEntity, frontEndQuery);
        Restriction innerRestriction = translateFrontEndRestriction(innerFrontEndRestriction, true,
                useDepivotedPhTable);
        return addSubselectRestriction(outerEntity, outerRestriction, innerEntity, innerRestriction);
    }

    Restriction joinRestrictions(Restriction outerRestriction, Restriction innerRestriction) {
        return (innerRestriction == null) ? outerRestriction
                : Restriction.builder().and(outerRestriction, innerRestriction).build();
    }

    private Restriction translateBucketRestriction(Restriction restriction, boolean translatePriorOnly,
            boolean useDepivotedPhTable) {
        Restriction translated = null;
        if (restriction instanceof LogicalRestriction) {
            BreadthFirstSearch search = new BreadthFirstSearch();
            search.run(restriction, (object, ctx) -> {
                if (object instanceof BucketRestriction) {
                    BucketRestriction bucket = (BucketRestriction) object;
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    parent.getRestrictions().remove(bucket);
                    if (Boolean.TRUE.equals(bucket.getIgnored())) {
                        log.warn("Ignored buckets should be filtered out by optimizer: " + JsonUtils.serialize(bucket));
                    } else {
                        Restriction converted = translateSingleBucket(bucket, repository, translatePriorOnly,
                                useDepivotedPhTable);
                        parent.getRestrictions().add(converted);
                    }
                } else if (object instanceof ConcreteRestriction) {
                    Restriction converted = RestrictionUtils.convertConcreteRestriction((ConcreteRestriction) object);
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    parent.getRestrictions().remove(object);
                    parent.getRestrictions().add(converted);
                }
            });
            translated = restriction;
        } else if (restriction instanceof BucketRestriction) {
            BucketRestriction bucket = (BucketRestriction) restriction;
            if (!Boolean.TRUE.equals(bucket.getIgnored())) {
                translated = translateSingleBucket(bucket, repository, translatePriorOnly, useDepivotedPhTable);
            } else {
                log.warn("Ignored buckets should be filtered out by optimizer: " + JsonUtils.serialize(bucket));
            }
        } else if (restriction instanceof ConcreteRestriction) {
            translated = RestrictionUtils.convertConcreteRestriction((ConcreteRestriction) restriction);
        } else {
            translated = restriction;
        }

        return translated;
    }

    private Restriction translateSingleBucket(BucketRestriction bucket, AttributeRepository attrRepo,
            boolean translatePriorOnly, boolean useDepivotedPhTable) {
        Restriction translated;
        if (BusinessEntity.PurchaseHistory.equals(bucket.getAttr().getEntity()) && //
                bucket.getBkt().getTransaction() == null) {
            translated = MetricTranslator.convert(bucket, useDepivotedPhTable);
        } else {
            ColumnMetadata cm = attrRepo == null ? null : attrRepo.getColumnMetadata(bucket.getAttr());
            if (RestrictionUtils.isUnencodedBoolean(cm)) {
                translated = RestrictionUtils.convertUnencodedBooleanBucketRestriction(bucket, cm, translatePriorOnly);
            } else {
                translated = RestrictionUtils.convertBucketRestriction(bucket, translatePriorOnly);
            }
        }
        return translated;
    }

    private void translateBigListRestrictions(Restriction restriction) {
        if (restriction instanceof LogicalRestriction) {
            BreadthFirstSearch search = new BreadthFirstSearch();
            search.run(restriction, (object, ctx) -> {
                if (object instanceof ConcreteRestriction) {
                    convertBigListRestriction((ConcreteRestriction) object);
                }
            });
        } else if (restriction instanceof ConcreteRestriction) {
            convertBigListRestriction((ConcreteRestriction) restriction);
        }
    }

    private void convertBigListRestriction(ConcreteRestriction restriction) {
        if (RestrictionUtils.isMultiValueOperator(restriction.getRelation())
                && (restriction.getRhs() instanceof CollectionLookup)
                && (restriction.getLhs() instanceof AttributeLookup)) {
            CollectionLookup collectionLookup = (CollectionLookup) restriction.getRhs();
            Collection<Object> vals = collectionLookup.getValues();
            if (CollectionUtils.size(vals) >= MAX_IN_LINE_COLLECTION_SIZE) {
                AttributeLookup attributeLookup = (AttributeLookup) restriction.getLhs();
                ColumnMetadata cm = repository.getColumnMetadata(attributeLookup);
                Class<?> fieldClz;
                if (cm != null) {
                    if (StringUtils.isBlank(cm.getJavaClass())) {
                        fieldClz = String.class;
                    } else {
                        fieldClz = AvroUtils.getJavaType(AvroUtils.getAvroType(cm.getJavaClass()));
                    }
                } else {
                    fieldClz = TempListUtils.getFieldClz(vals);
                }
                try {
                    if (tempListService == null) {
                        throw new NullPointerException("tempListService is null");
                    }
                    String tempTableName = tempListService.createTempListIfNotExists(restriction, fieldClz,
                            repository.getRedshiftPartition());

                    if (SPARK_BATCH_USER.equals(sqlUser)
                            && ComparisonType.NOT_IN_COLLECTION.equals(restriction.getRelation())) {
                        BusinessEntity entity = attributeLookup.getEntity();
                        StringPath entityTable = AttrRepoUtils.getTablePath(repository, entity);
                        String idAttrStr = entity.getServingStore().getPrimaryKey();
                        EntityPath<String> tempTable = new PathBuilder<>(String.class, tempTableName);
                        StringPath lhs = Expressions.stringPath(entityTable, attributeLookup.getAttribute());
                        StringPath rhs = Expressions.stringPath(tempTable, TempListUtils.VALUE_COLUMN);
                        BooleanExpression joinPredicate = lhs.eq(rhs);
                        BooleanExpression filterPredicate = lhs.isNotNull().and(rhs.isNull());
                        SQLQuery<?> query = queryFactory.getQuery(repository, sqlUser) //
                                .select(Expressions.stringPath(idAttrStr)) //
                                .from(entityTable) //
                                .leftJoin(tempTable).on(joinPredicate) //
                                .where(filterPredicate);
                        SubQuery subQuery = new SubQuery();
                        subQuery.setSubQueryExpression(query);
                        restriction.setLhs(new AttributeLookup(entity, idAttrStr));
                        restriction.setRelation(ComparisonType.IN_COLLECTION);
                        restriction.setRhs(new SubQueryAttrLookup(subQuery));
                    } else {
                        SubQuery subQuery = new SubQuery(tempTableName);
                        restriction.setRhs(new SubQueryAttrLookup(subQuery, TempListUtils.VALUE_COLUMN));
                    }
                } catch (NullPointerException e) {
                    throw e;
                }
            }
        }
    }

    // this is only used by non-event-table translations
    private Restriction specifyRestrictionParameters(Restriction restriction) {
        Restriction translated;
        if (restriction instanceof LogicalRestriction) {
            BreadthFirstSearch search = new BreadthFirstSearch();
            search.run(restriction, (object, ctx) -> {
                if (object instanceof TransactionRestriction) {
                    TransactionRestriction txRestriction = (TransactionRestriction) object;
                    modifyTxnRestriction(txRestriction, timeTranslator);
                    Restriction concrete = DateRangeTranslator.convert(txRestriction, queryFactory, repository,
                            sqlUser);
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    parent.getRestrictions().remove(txRestriction);
                    parent.getRestrictions().add(concrete);
                } else if (object instanceof DateRestriction) {
                    DateRestriction dateRestriction = (DateRestriction) object;
                    modifyDateRestriction(dateRestriction, timeTranslator);
                    Restriction concrete = DayRangeTranslator.convert(dateRestriction);
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    parent.getRestrictions().remove(dateRestriction);
                    parent.getRestrictions().add(concrete);
                }
            });
            translated = restriction;
        } else if (restriction instanceof TransactionRestriction) {
            TransactionRestriction txRestriction = (TransactionRestriction) restriction;
            modifyTxnRestriction(txRestriction, timeTranslator);
            translated = DateRangeTranslator.convert(txRestriction, queryFactory, repository, sqlUser);
        } else if (restriction instanceof DateRestriction) {
            DateRestriction dateRestriction = (DateRestriction) restriction;
            modifyDateRestriction(dateRestriction, timeTranslator);
            translated = DayRangeTranslator.convert(dateRestriction);
        } else {
            translated = restriction;
        }
        return translated;
    }

    private static void modifyTxnRestriction(TransactionRestriction txRestriction,
            TimeFilterTranslator timeTranslator) {
        if (timeTranslator == null) {
            throw new NullPointerException("TimeTranslator cannot be null.");
        }
        txRestriction.setTimeFilter(timeTranslator.translate(txRestriction.getTimeFilter()));
        if (txRestriction.getUnitFilter() != null) {
            txRestriction.setUnitFilter(setAggToSum(txRestriction.getUnitFilter()));
        }
        if (txRestriction.getSpentFilter() != null) {
            txRestriction.setSpentFilter(setAggToSum(txRestriction.getSpentFilter()));
        }
    }

    private static void modifyDateRestriction(DateRestriction dateRestriction, TimeFilterTranslator timeTranslator) {
        if (timeTranslator == null) {
            throw new NullPointerException("TimeTranslator cannot be null.");
        }
        dateRestriction
                .setTimeFilter(timeTranslator.translate(dateRestriction.getTimeFilter(), dateRestriction.getAttr()));
    }

    private static AggregationFilter setAggToSum(AggregationFilter filter) {
        return new AggregationFilter(filter.getSelector(), AggregationType.SUM, //
                filter.getComparisonType(), filter.getValues(), filter.isIncludeNotPurchased());
    }

    static Sort translateFrontEndSort(FrontEndSort frontEndSort) {
        if (frontEndSort != null) {
            return new Sort(frontEndSort.getAttributes(), Boolean.TRUE.equals(frontEndSort.getDescending()));
        } else {
            return null;
        }
    }

    Restriction addSubselectRestriction(BusinessEntity outerEntity, Restriction outerRestriction,
            BusinessEntity innerEntity, Restriction innerRestriction) {
        if (innerRestriction != null) {
            BusinessEntity.Relationship relationship = outerEntity.join(innerEntity);
            if (relationship == null || CollectionUtils.isEmpty(relationship.getJoinKeys())) {
                throw new IllegalArgumentException(
                        "Cannot find join keys between  " + outerEntity + " and " + innerEntity);
            }
            List<Pair<InterfaceName, InterfaceName>> joinKeys = relationship.getJoinKeys();
            if (joinKeys.size() != 1) {
                throw new UnsupportedOperationException("Can only handle entities joined by single key, but "
                        + outerEntity + " and " + innerEntity + " are joined by " + joinKeys.size() + " keys.");
            }
            String lhsKey = joinKeys.stream().map(Pair::getLeft).map(InterfaceName::name).findFirst().orElse(null);
            String rhsKey = joinKeys.stream().map(Pair::getRight).map(InterfaceName::name).findFirst().orElse(null);
            Query innerQuery = Query.builder().from(innerEntity).where(innerRestriction).select(innerEntity, lhsKey)
                    .build();
            SubQuery subQuery = new SubQuery(innerQuery, NamingUtils.randomSuffix(innerEntity.name(), 6));
            innerRestriction = Restriction.builder().let(outerEntity, rhsKey).inSubquery(subQuery).build();
        }
        return joinRestrictions(outerRestriction, innerRestriction);
    }

    void configurePagination(FrontEndQuery frontEndQuery) {
        if (frontEndQuery.getPageFilter() != null) {
            int rowSize = CollectionUtils.isNotEmpty(frontEndQuery.getLookups()) ? frontEndQuery.getLookups().size()
                    : 1;
            int maxRows = Math.floorDiv(MAX_CARDINALITY, rowSize);
            if (!isSparkQuery() && frontEndQuery.getPageFilter().getNumRows() > maxRows) {
                log.warn(String.format("Refusing to accept a query requesting more than %s rows."
                        + " Currently specified page filter: %s", maxRows, frontEndQuery.getPageFilter()));
                frontEndQuery.getPageFilter().setNumRows(maxRows);
            }
        }
    }

    protected boolean isSparkQuery() {
        return SPARK_BATCH_USER.equalsIgnoreCase(sqlUser);
    }

}
