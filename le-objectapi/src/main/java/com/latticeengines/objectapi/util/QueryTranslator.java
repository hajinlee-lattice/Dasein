package com.latticeengines.objectapi.util;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.graph.traversal.impl.BreadthFirstSearch;
import com.latticeengines.common.exposed.graph.traversal.impl.DepthFirstSearch;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.CaseLookup;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.QueryBuilder;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.Sort;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.domain.exposed.util.RestrictionOptimizer;
import com.latticeengines.domain.exposed.util.RestrictionUtils;
import com.latticeengines.query.exposed.translator.TransactionRestrictionTranslator;

public class QueryTranslator {
    private static final Logger log = LoggerFactory.getLogger(QueryTranslator.class);

    public static final int MAX_ROWS = 250;
    private static final PageFilter DEFAULT_PAGE_FILTER = new PageFilter(0, 100);

    public static Query translate(FrontEndQuery frontEndQuery, QueryDecorator decorator) {
        BusinessEntity mainEntity = frontEndQuery.getMainEntity();

        Map<String, Lookup> ruleBasedModels = ruleBasedModels(mainEntity, frontEndQuery.getRatingModels());

        Restriction restriction = translateFrontEndRestriction(mainEntity,
                getEntityFrontEndRestriction(mainEntity, frontEndQuery), ruleBasedModels);

        restriction = translateSalesforceIdRestriction(frontEndQuery, mainEntity, restriction);

        restriction = translateInnerRestriction(frontEndQuery, mainEntity, restriction, ruleBasedModels);

        if (frontEndQuery.getPageFilter() == null) {
            frontEndQuery.setPageFilter(DEFAULT_PAGE_FILTER);
        } else {
            if (frontEndQuery.getPageFilter().getNumRows() > MAX_ROWS) {
                log.warn(String.format("Refusing to accept a query requesting more than %s rows."
                        + " Currently specified page filter: %s", MAX_ROWS, frontEndQuery.getPageFilter()));
                frontEndQuery.getPageFilter().setNumRows(MAX_ROWS);
            }
        }

        QueryBuilder queryBuilder = Query.builder().from(mainEntity).where(restriction) //
                .orderBy(translateFrontEndSort(frontEndQuery.getSort())) //
                .page(frontEndQuery.getPageFilter());

        if (frontEndQuery.getLookups() != null && !frontEndQuery.getLookups().isEmpty()) {
            frontEndQuery.getLookups().forEach(lookup -> {
                AttributeLookup attributeLookup = (AttributeLookup) lookup;
                if (BusinessEntity.Rating.equals(attributeLookup.getEntity())) {
                    queryBuilder.select(parseRatingLookup(frontEndQuery.getMainEntity(), attributeLookup,
                            frontEndQuery.getRatingModels()));
                } else {
                    queryBuilder.select(attributeLookup.getEntity(), attributeLookup.getAttribute());
                }
            });
        } else if (decorator != null) {
            if (decorator.addSelects()) {
                queryBuilder.select(decorator.getAttributeLookups());
                if (frontEndQuery.getRatingModels() != null) {
                    frontEndQuery.getRatingModels().forEach(model -> {
                        if (model instanceof RuleBasedModel) {
                            String alias = model.getId();
                            if (frontEndQuery.getRatingModels().size() == 1) {
                                alias = "Score";
                            }
                            Lookup ruleLookup = translateRatingRule(frontEndQuery.getMainEntity(),
                                    ((RuleBasedModel) model).getRatingRule(), alias, false);
                            queryBuilder.select(ruleLookup);
                        } else {
                            log.warn("Cannot not handle rating model of type " + model.getClass().getSimpleName());
                        }
                    });
                }
            }
            queryBuilder.freeText(frontEndQuery.getFreeFormTextSearch(), decorator.getFreeTextSearchEntity(),
                    decorator.getFreeTextSearchAttrs());
        }

        return queryBuilder.build();
    }

    private static FrontEndRestriction getEntityFrontEndRestriction(BusinessEntity entity,
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

    private static Restriction translateSalesforceIdRestriction(FrontEndQuery frontEndQuery, BusinessEntity entity,
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

    private static Restriction translateInnerRestriction(FrontEndQuery frontEndQuery, BusinessEntity outerEntity,
            Restriction outerRestriction, Map<String, Lookup> ruleBasedModels) {
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
        Restriction innerRestriction = translateFrontEndRestriction(innerEntity, innerFrontEndRestriction,
                ruleBasedModels);
        return addExistsRestriction(outerRestriction, innerEntity, innerRestriction);
    }

    private static Restriction joinRestrictions(Restriction outerRestriction, Restriction innerRestriction) {
        return (innerRestriction == null) ? outerRestriction
                : Restriction.builder().and(outerRestriction, innerRestriction).build();
    }

    private static Restriction translateFrontEndRestriction(BusinessEntity entity,
            FrontEndRestriction frontEndRestriction, Map<String, Lookup> ruleBasedModels) {
        if (frontEndRestriction == null || frontEndRestriction.getRestriction() == null) {
            return null;
        }

        Restriction restriction = frontEndRestriction.getRestriction();

        Restriction translated;
        if (restriction instanceof LogicalRestriction) {
            BreadthFirstSearch search = new BreadthFirstSearch();
            search.run(restriction, (object, ctx) -> {
                if (object instanceof BucketRestriction) {
                    BucketRestriction bucket = (BucketRestriction) object;
                    Restriction converted = RestrictionUtils.convertBucketRestriction(bucket);
                    if (converted instanceof TransactionRestriction) {
                        converted = new TransactionRestrictionTranslator((TransactionRestriction) converted)
                                .convert(entity);
                    }
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    parent.getRestrictions().remove(bucket);
                    parent.getRestrictions().add(converted);
                } else if (object instanceof TransactionRestriction) {
                    TransactionRestriction txRestriction = (TransactionRestriction) object;
                    Restriction concrete = new TransactionRestrictionTranslator(txRestriction).convert(entity);
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    parent.getRestrictions().remove(txRestriction);
                    parent.getRestrictions().add(concrete);
                }
            });
            translated = restriction;
        } else if (restriction instanceof BucketRestriction) {
            BucketRestriction bucket = (BucketRestriction) restriction;
            translated = RestrictionUtils.convertBucketRestriction(bucket);
            if (translated instanceof TransactionRestriction) {
                translated = new TransactionRestrictionTranslator((TransactionRestriction) translated)
                        .convert(entity);
            }
        } else if (restriction instanceof TransactionRestriction) {
            TransactionRestriction txRestriction = (TransactionRestriction) restriction;
            translated = new TransactionRestrictionTranslator(txRestriction).convert(entity);
        } else {
            translated = restriction;
        }
        Restriction optimized = RestrictionOptimizer.optimize(translated);

        if (optimized != null) {
            DepthFirstSearch dfs = new DepthFirstSearch();
            dfs.run(optimized, (obj, ctx) -> {
                if (obj instanceof ConcreteRestriction) {
                    translateRuleBasedRating((Restriction) obj, ruleBasedModels);
                }
            });
        }

        return optimized;
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

    private static Restriction addExistsRestriction(Restriction outerRestriction, BusinessEntity innerEntity,
            Restriction innerRestriction) {
        Restriction existsRestriction = null;
        if (innerRestriction != null) {
            existsRestriction = Restriction.builder().exists(innerEntity).that(innerRestriction).build();
        }
        return joinRestrictions(outerRestriction, existsRestriction);
    }

    public static Lookup translateRatingRule(BusinessEntity entity, RatingRule ratingRule, String alias,
            boolean forScoreCount) {
        TreeMap<String, Restriction> cases = new TreeMap<>();
        AtomicInteger idx = new AtomicInteger(0);
        ratingRule.getBucketToRuleMap().forEach((key, val) -> {
            FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
            frontEndRestriction.setRestriction(val.get(FrontEndQueryConstants.ACCOUNT_RESTRICTION));
            // do not support nested ratings for now
            Restriction accountRestriction = translateFrontEndRestriction(BusinessEntity.Account, frontEndRestriction,
                    null);

            frontEndRestriction = new FrontEndRestriction();
            frontEndRestriction.setRestriction(val.get(FrontEndQueryConstants.CONTACT_RESTRICTION));
            // do not support nested ratings for now
            Restriction contactRestriction = translateFrontEndRestriction(BusinessEntity.Contact, frontEndRestriction,
                    null);

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

    private static Map<String, Lookup> ruleBasedModels(BusinessEntity mainEntity, List<RatingModel> models) {
        Map<String, Lookup> lookupMap = new ConcurrentHashMap<>();
        if (models != null) {
            BusinessEntity entity = mainEntity != null ? mainEntity : BusinessEntity.Account;
            models.forEach(model -> {
                if (model instanceof RuleBasedModel) {
                    RatingRule ratingRule = ((RuleBasedModel) model).getRatingRule();
                    String modelId = model.getId();
                    Lookup lookup = translateRatingRule(entity, ratingRule, modelId, false);
                    lookupMap.put(modelId, lookup);
                }
            });
        }
        return lookupMap;
    }

    private static Lookup parseRatingLookup(BusinessEntity entity, AttributeLookup lookup, List<RatingModel> models) {
        if (models == null) {
            throw new RuntimeException(
                    "You specified a rating lookup " + lookup + " but no rating models, cannot parse the lookup.");
        }
        RatingModel model = models.stream().filter(m -> lookup.getAttribute().equalsIgnoreCase(m.getId())).findFirst()
                .orElse(null);
        if (model != null) {
            if (models.get(0) instanceof RuleBasedModel) {
                RatingRule ratingRule = ((RuleBasedModel) models.get(0)).getRatingRule();
                return translateRatingRule(entity, ratingRule, lookup.getAttribute(), false);
            } else {
                throw new UnsupportedOperationException("Only support rule based model now.");
            }
        } else {
            throw new RuntimeException("Cannot find a rating model with id=" + lookup.getAttribute());
        }
    }

}
