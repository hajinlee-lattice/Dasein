package com.latticeengines.objectapi.util;

import static com.latticeengines.query.exposed.translator.TranslatorUtils.generateAlias;

import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.graph.traversal.impl.BreadthFirstSearch;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AggregationType;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.QueryBuilder;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.Sort;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.domain.exposed.util.RestrictionOptimizer;
import com.latticeengines.domain.exposed.util.RestrictionUtils;
import com.latticeengines.domain.exposed.util.TimeFilterTranslator;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.latticeengines.query.exposed.translator.DateRangeTranslator;

abstract class QueryTranslator {

    private static final Logger log = LoggerFactory.getLogger(QueryTranslator.class);

    static final int MAX_CARDINALITY = 20000;
    static final PageFilter DEFAULT_PAGE_FILTER = new PageFilter(0, 100);

    QueryFactory queryFactory;
    AttributeRepository repository;

    QueryTranslator(QueryFactory queryFactory, AttributeRepository repository) {
        this.queryFactory = queryFactory;
        this.repository = repository;
    }

    Query translateProductQuery(FrontEndQuery frontEndQuery, QueryDecorator decorator) {
        QueryBuilder queryBuilder = Query.builder();
        BusinessEntity mainEntity = BusinessEntity.Product;

        frontEndQuery.setAccountRestriction(null);
        frontEndQuery.setContactRestriction(null);
        frontEndQuery.setRatingModels(null);
        Restriction restriction = Restriction.builder().and(Collections.emptyList()).build();

        queryBuilder.from(mainEntity).where(restriction) //
                .orderBy(translateFrontEndSort(frontEndQuery.getSort())) //
                .page(frontEndQuery.getPageFilter()) //
                .freeText(frontEndQuery.getFreeFormTextSearch(), decorator.getFreeTextSearchEntity(),
                        decorator.getFreeTextSearchAttrs());

        if (decorator.addSelects()) {
            queryBuilder.select(decorator.getAttributeLookups());
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

    Restriction translateFrontEndRestriction(FrontEndRestriction frontEndRestriction) {
        if (frontEndRestriction == null || frontEndRestriction.getRestriction() == null) {
            return null;
        }
        Restriction restriction = translateBucketRestriction(frontEndRestriction.getRestriction());
        return RestrictionOptimizer.optimize(restriction);
    }

    Restriction translateFrontEndRestriction(BusinessEntity entity, FrontEndRestriction frontEndRestriction,
            QueryBuilder queryBuilder, TimeFilterTranslator timeTranslator) {
        Restriction restriction = translateFrontEndRestriction(frontEndRestriction);
        if (restriction == null) {
            return null;
        }

        Restriction translated = translateTransactionRestriction(entity, restriction, queryBuilder, timeTranslator);
        return RestrictionOptimizer.optimize(translated);
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

    Restriction translateInnerRestriction(FrontEndQuery frontEndQuery, BusinessEntity outerEntity,
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
        Restriction innerRestriction = translateFrontEndRestriction(innerFrontEndRestriction);
        return addSubselectRestriction(outerEntity, outerRestriction, innerEntity, innerRestriction);
    }

    Restriction joinRestrictions(Restriction outerRestriction, Restriction innerRestriction) {
        return (innerRestriction == null) ? outerRestriction
                : Restriction.builder().and(outerRestriction, innerRestriction).build();
    }

    private Restriction translateBucketRestriction(Restriction restriction) {
        Restriction translated = null;
        if (restriction instanceof LogicalRestriction) {
            BreadthFirstSearch search = new BreadthFirstSearch();
            search.run(restriction, (object, ctx) -> {
                if (object instanceof BucketRestriction) {
                    BucketRestriction bucket = (BucketRestriction) object;
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");

                    if (Boolean.TRUE.equals(bucket.getIgnored())) {
                        parent.getRestrictions().remove(bucket);
                        log.warn("Ignored buckets should be filtered out by optimizer: " + JsonUtils.serialize(bucket));
                    } else {
                        Restriction converted = RestrictionUtils.convertBucketRestriction(bucket);
                        parent.getRestrictions().remove(bucket);
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
                translated = RestrictionUtils.convertBucketRestriction(bucket);
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

    // this is only used by non-event-table translations
    private Restriction translateTransactionRestriction(BusinessEntity entity, Restriction restriction, //
            QueryBuilder queryBuilder, TimeFilterTranslator timeTranslator) {
        // // translateRange mutli-product "has engaged" to logical grouping
        // if (restriction instanceof LogicalRestriction) {
        // BreadthFirstSearch bfs = new BreadthFirstSearch();
        // bfs.run(restriction, (object, ctx) -> {
        // if (object instanceof TransactionRestriction) {
        // TransactionRestriction txRestriction = (TransactionRestriction)
        // object;
        // if
        // (TransactionRestrictionTranslator.isHasEngagedRestriction(txRestriction))
        // {
        // Restriction newRestriction =
        // TransactionRestrictionTranslator.translateHasEngagedToLogicalGroup(txRestriction);
        // LogicalRestriction parent = (LogicalRestriction)
        // ctx.getProperty("parent");
        // parent.getRestrictions().remove(txRestriction);
        // parent.getRestrictions().add(newRestriction);
        // }
        // }
        // });
        // } else if (restriction instanceof TransactionRestriction) {
        // TransactionRestriction txRestriction = (TransactionRestriction)
        // restriction;
        //
        // if
        // (TransactionRestrictionTranslator.isHasEngagedRestriction(txRestriction))
        // {
        // restriction =
        // TransactionRestrictionTranslator.translateHasEngagedToLogicalGroup(txRestriction);
        // }
        // }

        Restriction translated;
        if (restriction instanceof LogicalRestriction) {
            BreadthFirstSearch search = new BreadthFirstSearch();
            search.run(restriction, (object, ctx) -> {
                if (object instanceof TransactionRestriction) {
                    TransactionRestriction txRestriction = (TransactionRestriction) object;
                    modifyTxnRestriction(txRestriction, timeTranslator);
                    Restriction concrete = new DateRangeTranslator().convert(txRestriction, queryFactory, repository);
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    parent.getRestrictions().remove(txRestriction);
                    parent.getRestrictions().add(concrete);
                }
            });
            translated = restriction;
        } else if (restriction instanceof TransactionRestriction) {
            TransactionRestriction txRestriction = (TransactionRestriction) restriction;
            modifyTxnRestriction(txRestriction, timeTranslator);
            translated = new DateRangeTranslator().convert(txRestriction, queryFactory, repository);
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

    private static AggregationFilter setAggToSum(AggregationFilter filter) {
        return new AggregationFilter(filter.getSelector(), AggregationType.SUM, //
                filter.getComparisonType(), filter.getValues());
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
            SubQuery subQuery = new SubQuery(innerQuery, generateAlias(innerEntity.name()));
            innerRestriction = Restriction.builder().let(outerEntity, rhsKey).inCollection(subQuery, lhsKey).build();
        }
        return joinRestrictions(outerRestriction, innerRestriction);
    }

    void configurePagination(FrontEndQuery frontEndQuery) {
        if (frontEndQuery.getPageFilter() == null) {
            frontEndQuery.setPageFilter(DEFAULT_PAGE_FILTER);
        } else {
            int rowSize = CollectionUtils.isNotEmpty(frontEndQuery.getLookups()) ? frontEndQuery.getLookups().size()
                    : 1;
            int maxRows = Math.floorDiv(MAX_CARDINALITY, rowSize);
            if (frontEndQuery.getPageFilter().getNumRows() > maxRows) {
                log.warn(String.format("Refusing to accept a query requesting more than %s rows."
                        + " Currently specified page filter: %s", maxRows, frontEndQuery.getPageFilter()));
                frontEndQuery.getPageFilter().setNumRows(maxRows);
            }
        }
    }

}
