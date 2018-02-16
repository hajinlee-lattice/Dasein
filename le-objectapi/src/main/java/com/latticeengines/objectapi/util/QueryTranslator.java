package com.latticeengines.objectapi.util;

import static com.latticeengines.query.exposed.translator.TranslatorUtils.generateAlias;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.graph.traversal.impl.BreadthFirstSearch;
import com.latticeengines.common.exposed.graph.traversal.impl.DepthFirstSearch;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.CaseLookup;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.EventType;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.QueryBuilder;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.Sort;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.domain.exposed.util.RestrictionOptimizer;
import com.latticeengines.domain.exposed.util.RestrictionUtils;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.latticeengines.query.exposed.translator.EventQueryTranslator;
import com.latticeengines.query.exposed.translator.TransactionRestrictionTranslator;

public class QueryTranslator {
    private static final Logger log = LoggerFactory.getLogger(QueryTranslator.class);

    private static final int MAX_CARDINALITY = 20000;
    private static final PageFilter DEFAULT_PAGE_FILTER = new PageFilter(0, 100);

    private QueryFactory queryFactory;
    private AttributeRepository repository;

    public QueryTranslator(QueryFactory queryFactory, AttributeRepository repository) {
        this.queryFactory = queryFactory;
        this.repository = repository;
    }

    public Query translateModelingEvent(EventFrontEndQuery frontEndQuery, EventType eventType) {
        FrontEndRestriction frontEndRestriction = getEntityFrontEndRestriction(BusinessEntity.Account, frontEndQuery);

        if (frontEndRestriction == null || frontEndRestriction.getRestriction() == null) {
            throw new IllegalArgumentException("No restriction specified for event query");
        }


        EventQueryTranslator eventQueryTranslator = new EventQueryTranslator();
        QueryBuilder queryBuilder = Query.builder();
        Map<String, Lookup> ruleBasedModels = ruleBasedModels(BusinessEntity.Account, //
                                                              frontEndQuery.getRatingModels(), //
                                                              queryBuilder, true);
        Restriction restriction = translateFrontEndRestriction(BusinessEntity.Account,
                                                               frontEndRestriction,
                                                               ruleBasedModels, queryBuilder, true);
        restriction = translateSalesforceIdRestriction(frontEndQuery, BusinessEntity.Account, restriction);
        restriction = translateInnerRestriction(frontEndQuery, BusinessEntity.Account, restriction, null, //
                                                queryBuilder, true);
        setTargetProducts(restriction, frontEndQuery.getTargetProductIds());

        switch (eventType) {
        case Scoring:
            queryBuilder = eventQueryTranslator.translateForScoring(queryFactory, repository, restriction,
                                                                    frontEndQuery,
                                                                    queryBuilder);
            break;
        case Training:
            queryBuilder = eventQueryTranslator.translateForTraining(queryFactory, repository, restriction,
                                                                     frontEndQuery,
                                                                     queryBuilder);
            break;
        case Event:
            queryBuilder = eventQueryTranslator.translateForEvent(queryFactory, repository, restriction,
                                                                  frontEndQuery,
                                                                  queryBuilder);
            break;
        }

        PageFilter pageFilter = new PageFilter(0, 0);

        if (frontEndQuery.getPageFilter() != null) {
            pageFilter = frontEndQuery.getPageFilter();
        }

        queryBuilder.page(pageFilter);

        if (pageFilter.getRowOffset() > 0 || pageFilter.getNumRows() > 0) {
            // set sort order, or pagination will return different result each time
            Sort sort = new Sort();
            sort.setLookups(queryBuilder.getLookups());
            queryBuilder.orderBy(sort);
        }

        return queryBuilder.build();
    }

    public Query translate(FrontEndQuery frontEndQuery, QueryDecorator decorator) {
        BusinessEntity mainEntity = frontEndQuery.getMainEntity();

        Restriction restriction;
        QueryBuilder queryBuilder = Query.builder();

        if (BusinessEntity.Product.equals(mainEntity)) {
            frontEndQuery.setAccountRestriction(null);
            frontEndQuery.setContactRestriction(null);
            frontEndQuery.setRatingModels(null);
            restriction = Restriction.builder().and(Collections.emptyList()).build();
        } else {
            Map<String, Lookup> ruleBasedModels = ruleBasedModels(mainEntity, frontEndQuery.getRatingModels(), //
                                                                  queryBuilder, false);
            restriction = translateFrontEndRestriction(mainEntity,
                                                       getEntityFrontEndRestriction(mainEntity, frontEndQuery),
                                                       ruleBasedModels, queryBuilder, false);
            restriction = translateSalesforceIdRestriction(frontEndQuery, mainEntity, restriction);
            restriction = translateInnerRestriction(frontEndQuery, mainEntity, restriction, ruleBasedModels, //
                                                    queryBuilder, false);
            restriction = addHasTransactionRestriction(frontEndQuery, restriction);
        }

        queryBuilder.from(mainEntity).where(restriction) //
                .orderBy(translateFrontEndSort(frontEndQuery.getSort())) //
                .page(frontEndQuery.getPageFilter());

        if (frontEndQuery.getLookups() != null && !frontEndQuery.getLookups().isEmpty()) {
            frontEndQuery.getLookups().forEach(lookup -> {
                AttributeLookup attributeLookup = (AttributeLookup) lookup;
                if (BusinessEntity.Rating.equals(attributeLookup.getEntity())) {
                    queryBuilder.select(parseRatingLookup(frontEndQuery.getMainEntity(), attributeLookup,
                                                          frontEndQuery.getRatingModels(), queryBuilder, false));
                } else {
                    queryBuilder.select(attributeLookup.getEntity(), attributeLookup.getAttribute());
                }
            });
        } else if (decorator != null) {
            if (decorator.addSelects()) {
                queryBuilder.select(decorator.getAttributeLookups());
                if (frontEndQuery.getRatingModels() != null) {
                    appendRuleLookups(frontEndQuery, queryBuilder);
                }
            }
        }

        if (decorator != null) {
            queryBuilder.freeText(frontEndQuery.getFreeFormTextSearch(), decorator.getFreeTextSearchEntity(),
                    decorator.getFreeTextSearchAttrs());
        }

        if (!BusinessEntity.Product.equals(mainEntity)) {
            if (frontEndQuery.getPageFilter() == null) {
                frontEndQuery.setPageFilter(DEFAULT_PAGE_FILTER);
            } else {
                int rowSize = CollectionUtils.isNotEmpty(frontEndQuery.getLookups()) ? frontEndQuery.getLookups().size() : 1;
                int maxRows = Math.floorDiv(MAX_CARDINALITY, rowSize);
                if (frontEndQuery.getPageFilter().getNumRows() > maxRows) {
                    log.warn(String.format("Refusing to accept a query requesting more than %s rows."
                            + " Currently specified page filter: %s", maxRows, frontEndQuery.getPageFilter()));
                    frontEndQuery.getPageFilter().setNumRows(maxRows);
                }
            }
        }

        return queryBuilder.build();
    }

    private void appendRuleLookups(FrontEndQuery frontEndQuery, QueryBuilder queryBuilder) {
        frontEndQuery.getRatingModels().forEach(model -> {
            if (model instanceof RuleBasedModel) {
                String alias = model.getId();
                if (frontEndQuery.getRatingModels().size() == 1) {
                    alias = "Score";
                }
                Lookup ruleLookup = translateRatingRule(frontEndQuery.getMainEntity(),
                                                        ((RuleBasedModel) model).getRatingRule(), alias, false,
                                                        queryBuilder, false);
                queryBuilder.select(ruleLookup);
            } else {
                log.warn("Cannot not handle rating model of type " + model.getClass().getSimpleName());
            }
        });
    }

    private FrontEndRestriction getEntityFrontEndRestriction(BusinessEntity entity,
                                                             FrontEndQuery frontEndQuery) {
        switch (entity) {
        case Account:
            return frontEndQuery.getAccountRestriction();
        case Contact:
            return frontEndQuery.getContactRestriction();
        default:
            return null;
        }
    }

    private Restriction translateSalesforceIdRestriction(FrontEndQuery frontEndQuery, BusinessEntity entity,
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

    private Restriction addHasTransactionRestriction(FrontEndQuery frontEndQuery, Restriction restriction) {
        // only apply has transaction restriction to account entity
        if (BusinessEntity.Account.equals(frontEndQuery.getMainEntity()) && !hasTransactionRestriction(restriction)) {
            if (Boolean.TRUE.equals(frontEndQuery.getRestrictHasTransaction())) {
                BusinessEntity innerEntity = BusinessEntity.Transaction;
                Restriction innerRestriction = Restriction.builder()
                        .let(BusinessEntity.Transaction, InterfaceName.AccountId.name()).isNotNull().build();
                Restriction hasTransaction = addSubselectRestriction(BusinessEntity.Account,
                                                                     null,
                                                                     innerEntity,
                                                                     innerRestriction,
                                                                     InterfaceName.AccountId.name());
                return joinRestrictions(restriction, hasTransaction);
            }
        }
        return restriction;
    }

    private boolean hasTransactionRestriction(Restriction rootRestriction) {
        AtomicBoolean hasTxn = new AtomicBoolean(false);
        if (rootRestriction instanceof LogicalRestriction) {
            BreadthFirstSearch bfs = new BreadthFirstSearch();
            bfs.run(rootRestriction, (object, ctx) -> {
                if (object instanceof TransactionRestriction) {
                    hasTxn.set(true);
                }
            });
        } else if (rootRestriction instanceof TransactionRestriction) {
            hasTxn.set(true);
        }
        return hasTxn.get();
    }

    private Restriction translateInnerRestriction(FrontEndQuery frontEndQuery, BusinessEntity outerEntity,
                                                  Restriction outerRestriction, Map<String, Lookup> ruleBasedModels,
                                                  QueryBuilder queryBuilder, boolean ignoreTransaction) {
        BusinessEntity innerEntity = null;
        String joinEntityKey = null;
        switch (outerEntity) {
        case Contact:
            innerEntity = BusinessEntity.Account;
            joinEntityKey = InterfaceName.AccountId.name();
            break;
        case Account:
            innerEntity = BusinessEntity.Contact;
            joinEntityKey = InterfaceName.AccountId.name();
            break;
        default:
            break;
        }
        FrontEndRestriction innerFrontEndRestriction = getEntityFrontEndRestriction(innerEntity, frontEndQuery);
        Restriction innerRestriction = translateFrontEndRestriction(innerEntity, innerFrontEndRestriction,
                                                                    ruleBasedModels, queryBuilder, ignoreTransaction);
        return addSubselectRestriction(outerEntity, outerRestriction, innerEntity, innerRestriction, joinEntityKey);
    }

    private Restriction joinRestrictions(Restriction outerRestriction, Restriction innerRestriction) {
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

                    if (bucket.isDeleted()) {
                        parent.getRestrictions().remove(bucket);
                    } else {
                        Restriction converted = RestrictionUtils.convertBucketRestriction(bucket);
                        parent.getRestrictions().remove(bucket);
                        parent.getRestrictions().add(converted);
                    }
                }
            });
            translated = restriction;
        } else if (restriction instanceof BucketRestriction) {
            BucketRestriction bucket = (BucketRestriction) restriction;
            if (!bucket.isDeleted()) {
                translated = RestrictionUtils.convertBucketRestriction(bucket);
            }
        } else {
            translated = restriction;
        }

        return translated;
    }

    private Restriction translateFrontEndRestriction(BusinessEntity entity,
                                                     FrontEndRestriction frontEndRestriction,
                                                     Map<String, Lookup> ruleBasedModels,
                                                     QueryBuilder queryBuilder, boolean ignoreTransaction) {
        if (frontEndRestriction == null || frontEndRestriction.getRestriction() == null) {
            return null;
        }

        Restriction restriction = translateBucketRestriction(frontEndRestriction.getRestriction());

        Restriction translated = (ignoreTransaction)  //
                ? restriction
                : translateTransactionRestriction(entity, restriction, queryBuilder);
        Restriction optimized = RestrictionOptimizer.optimize(translated);

        translateRatingRuleRestriction(ruleBasedModels, optimized);

        return optimized;
    }

    // this is only used by non-event-table translations
    private Restriction translateTransactionRestriction(BusinessEntity entity, Restriction restriction, //
                                                        QueryBuilder queryBuilder) {
        // translate mutli-product "has engaged" to logical grouping
        if (restriction instanceof LogicalRestriction) {
            BreadthFirstSearch bfs = new BreadthFirstSearch();
            bfs.run(restriction, (object, ctx) -> {
                if (object instanceof TransactionRestriction) {
                    TransactionRestriction txRestriction = (TransactionRestriction) object;
                    if (TransactionRestrictionTranslator.isHasEngagedRestriction(txRestriction)) {
                        Restriction newRestriction = TransactionRestrictionTranslator.translateHasEngagedToLogicalGroup(txRestriction);
                        LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                        parent.getRestrictions().remove(txRestriction);
                        parent.getRestrictions().add(newRestriction);
                    }
                }
            });
        } else if (restriction instanceof TransactionRestriction) {
            TransactionRestriction txRestriction = (TransactionRestriction) restriction;

            if (TransactionRestrictionTranslator.isHasEngagedRestriction(txRestriction)) {
                restriction = TransactionRestrictionTranslator.translateHasEngagedToLogicalGroup(txRestriction);
            }
        }

        Restriction translated;
        if (restriction instanceof LogicalRestriction) {
            BreadthFirstSearch search = new BreadthFirstSearch();
            search.run(restriction, (object, ctx) -> {
                if (object instanceof TransactionRestriction) {
                    TransactionRestriction txRestriction = (TransactionRestriction) object;
                    Restriction concrete = new TransactionRestrictionTranslator()
                            .convert(txRestriction, queryFactory, repository, entity,
                                     queryBuilder);
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    parent.getRestrictions().remove(txRestriction);
                    parent.getRestrictions().add(concrete);
                }
            });
            translated = restriction;
        } else if (restriction instanceof TransactionRestriction) {
            TransactionRestriction txRestriction = (TransactionRestriction) restriction;
            translated = new TransactionRestrictionTranslator().convert(
                    txRestriction, queryFactory, repository, entity, queryBuilder);
        } else {
            translated = restriction;
        }
        return translated;
    }

    private void translateRatingRuleRestriction(Map<String, Lookup> ruleBasedModels, Restriction restriction) {
        if (restriction != null) {
            DepthFirstSearch dfs = new DepthFirstSearch();
            dfs.run(restriction, (obj, ctx) -> {
                if (obj instanceof ConcreteRestriction) {
                    translateRuleBasedRating((Restriction) obj, ruleBasedModels);
                }
            });
        }
    }

    private static Sort translateFrontEndSort(FrontEndSort frontEndSort) {
        if (frontEndSort != null) {
            return new Sort(frontEndSort.getAttributes(), Boolean.TRUE.equals(frontEndSort.getDescending()));
        } else {
            return null;
        }
    }

    private static void translateRuleBasedRating(Restriction restriction, Map<String, Lookup> ruleBasedModels) {
        if (ruleBasedModels != null && !ruleBasedModels.isEmpty()) {
            if (restriction instanceof ConcreteRestriction
                    && ((ConcreteRestriction) restriction).getLhs() instanceof AttributeLookup && BusinessEntity.Rating
                            .equals(((AttributeLookup) ((ConcreteRestriction) restriction).getLhs()).getEntity())) {
                AttributeLookup attributeLookup = (AttributeLookup) ((ConcreteRestriction) restriction).getLhs();
                String modelId = attributeLookup.getAttribute();
                log.info("Translating a concrete restriction involving rule based rating engine " + modelId);
                if (ruleBasedModels.containsKey(modelId)) {
                    Lookup ruleLookup = ruleBasedModels.get(modelId);
                    ((ConcreteRestriction) restriction).setLhs(ruleLookup);
                } else {
                    // TODO: handle analytic models
                    log.warn("Cannot find the definition of rule based model " + modelId + ".");
                }
            }
        }
    }

    private Restriction addSubselectRestriction(BusinessEntity outerEntity,
                                                Restriction outerRestriction,
                                                BusinessEntity innerEntity,
                                                Restriction innerRestriction,
                                                String joinEntityKey) {
        if (innerRestriction != null) {
            Query innerQuery = Query.builder().from(innerEntity)
                    .where(innerRestriction)
                    .select(innerEntity, joinEntityKey).build();
            SubQuery subQuery = new SubQuery(innerQuery, generateAlias(innerEntity.name()));
            innerRestriction = Restriction.builder().let(outerEntity, joinEntityKey)
                    .inCollection(subQuery, joinEntityKey).build();
        }
        return joinRestrictions(outerRestriction, innerRestriction);
    }

    private Restriction addExistsRestriction(Restriction outerRestriction, BusinessEntity innerEntity,
                                             Restriction innerRestriction) {
        Restriction existsRestriction = null;
        if (innerRestriction != null) {
            existsRestriction = Restriction.builder().exists(innerEntity).that(innerRestriction).build();
        }
        return joinRestrictions(outerRestriction, existsRestriction);
    }

    public Lookup translateRatingRule(BusinessEntity entity, RatingRule ratingRule, String alias,
                                      boolean forScoreCount, QueryBuilder queryBuilder, boolean ignoreTransaction) {
        TreeMap<String, Restriction> cases = new TreeMap<>();
        AtomicInteger idx = new AtomicInteger(0);
        ratingRule.getBucketToRuleMap().forEach((key, val) -> {
            FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
            frontEndRestriction.setRestriction(val.get(FrontEndQueryConstants.ACCOUNT_RESTRICTION));
            // do not support nested ratings for now
            Restriction accountRestriction = translateFrontEndRestriction(BusinessEntity.Account, frontEndRestriction,
                                                                          null, queryBuilder, ignoreTransaction);

            frontEndRestriction = new FrontEndRestriction();
            frontEndRestriction.setRestriction(val.get(FrontEndQueryConstants.CONTACT_RESTRICTION));
            // do not support nested ratings for now
            Restriction contactRestriction = translateFrontEndRestriction(BusinessEntity.Contact, frontEndRestriction,
                                                                          null, queryBuilder, ignoreTransaction);

            BusinessEntity innerEntity;
            Restriction outerRestriction, innerRestriction;
            if (BusinessEntity.Account.equals(entity)) {
                innerEntity = BusinessEntity.Contact;
                outerRestriction = accountRestriction;
                innerRestriction = contactRestriction;
            } else if (BusinessEntity.Contact.equals(entity)) {
                innerEntity = BusinessEntity.Account;
                outerRestriction = contactRestriction;
                innerRestriction = accountRestriction;
            } else {
                throw new RuntimeException("Cannot determine inner entity based on main entity " + entity);
            }

            if (forScoreCount) {
                cases.put(String.valueOf(idx.getAndIncrement()), joinRestrictions(outerRestriction, innerRestriction));
            } else {
                cases.put(key, addExistsRestriction(outerRestriction, innerEntity, innerRestriction));
            }
        });
        if (forScoreCount) {
            CaseLookup caseLookup = new CaseLookup(cases, String.valueOf(idx.get()), null);
            AggregateLookup lookup = AggregateLookup.min(caseLookup);
            lookup.setAlias(alias);
            return lookup;
        } else {
            return new CaseLookup(cases, ratingRule.getDefaultBucketName(), alias);
        }
    }

    private Map<String, Lookup> ruleBasedModels(BusinessEntity mainEntity, List<RatingModel> models, //
                                                QueryBuilder queryBuilder, boolean ignoreTransaction) {
        Map<String, Lookup> lookupMap = new ConcurrentHashMap<>();
        if (models != null) {
            BusinessEntity entity = mainEntity != null ? mainEntity : BusinessEntity.Account;
            models.forEach(model -> {
                if (model instanceof RuleBasedModel) {
                    RatingRule ratingRule = ((RuleBasedModel) model).getRatingRule();
                    String modelId = model.getId();
                    Lookup lookup = translateRatingRule(entity, ratingRule, modelId, false, queryBuilder,
                                                        ignoreTransaction);
                    lookupMap.put(modelId, lookup);
                }
            });
        }
        return lookupMap;
    }

    private Lookup parseRatingLookup(BusinessEntity entity, AttributeLookup lookup, List<RatingModel> models, //
                                     QueryBuilder queryBuilder, boolean ignoreTransaction) {
        if (models == null) {
            throw new RuntimeException(
                    "You specified a rating lookup " + lookup + " but no rating models, cannot parse the lookup.");
        }
        RatingModel model = models.stream().filter(m -> lookup.getAttribute().equalsIgnoreCase(m.getId())).findFirst()
                .orElse(null);
        if (model != null) {
            if (models.get(0) instanceof RuleBasedModel) {
                RatingRule ratingRule = ((RuleBasedModel) models.get(0)).getRatingRule();
                return translateRatingRule(entity, ratingRule, lookup.getAttribute(), false, queryBuilder,
                                           ignoreTransaction);
            } else {
                throw new UnsupportedOperationException("Only support rule based model now.");
            }
        } else {
            throw new RuntimeException("Cannot find a rating model with id=" + lookup.getAttribute());
        }
    }

    private void setTargetProducts(Restriction rootRestriction, List<String> targetProducts) {
        if (CollectionUtils.isNotEmpty(targetProducts)) {
            String concatenated = StringUtils.join(targetProducts, ",");
            if (rootRestriction instanceof LogicalRestriction) {
                BreadthFirstSearch bfs = new BreadthFirstSearch();
                bfs.run(rootRestriction, (object, ctx) -> {
                    if (object instanceof TransactionRestriction) {
                        TransactionRestriction txRestriction = (TransactionRestriction) object;
                        txRestriction.setTargetProductId(concatenated);
                    }
                });
            } else if (rootRestriction instanceof TransactionRestriction) {
                TransactionRestriction txRestriction = (TransactionRestriction) rootRestriction;
                txRestriction.setTargetProductId(concatenated);
            }
        }
    }

}
