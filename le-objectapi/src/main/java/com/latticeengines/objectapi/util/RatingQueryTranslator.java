package com.latticeengines.objectapi.util;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.graph.traversal.impl.DepthFirstSearch;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.CaseLookup;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.QueryBuilder;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.util.TimeFilterTranslator;
import com.latticeengines.query.exposed.factory.QueryFactory;

public class RatingQueryTranslator extends QueryTranslator {
    private static final Logger log = LoggerFactory.getLogger(RatingQueryTranslator.class);

    public RatingQueryTranslator(QueryFactory queryFactory, AttributeRepository repository) {
        super(queryFactory, repository);
    }

    public Query translateRatingQuery(FrontEndQuery frontEndQuery, AttributeRepository attrRepo, boolean isCountQuery, //
            TimeFilterTranslator timeTranslator, String sqlUser) {
        BusinessEntity mainEntity = frontEndQuery.getMainEntity();

        if (BusinessEntity.Product.equals(mainEntity)) {
            return translateProductQuery(frontEndQuery, isCountQuery);
        }

        Restriction restriction;
        QueryBuilder queryBuilder = Query.builder();

        Map<String, Lookup> ruleBasedModels = ruleBasedModels(mainEntity, attrRepo, frontEndQuery.getRatingModels(), //
                timeTranslator, sqlUser);
        restriction = translateFrontEndRestriction(getEntityFrontEndRestriction(mainEntity, frontEndQuery), //
                attrRepo, timeTranslator, sqlUser, true);
        translateRatingRuleRestriction(ruleBasedModels, restriction);
        restriction = translateSalesforceIdRestriction(frontEndQuery, mainEntity, restriction);
        restriction = translateInnerRestriction(frontEndQuery, attrRepo, mainEntity, restriction, ruleBasedModels, //
                timeTranslator, sqlUser);

        queryBuilder.from(mainEntity).where(restriction) //
                .orderBy(translateFrontEndSort(frontEndQuery.getSort())) //
                .page(frontEndQuery.getPageFilter()) //
                .distinct(frontEndQuery.getDistinct());

        AtomicBoolean hasRatingLookup = new AtomicBoolean(false);
        if (CollectionUtils.isNotEmpty(frontEndQuery.getLookups())) {
            frontEndQuery.getLookups().forEach(lookup -> {
                if (lookup instanceof AttributeLookup) {
                    AttributeLookup attributeLookup = (AttributeLookup) lookup;
                    if (BusinessEntity.Rating.equals(attributeLookup.getEntity())) {
                        hasRatingLookup.set(true);
                        queryBuilder.select(parseRatingLookup(frontEndQuery.getMainEntity(), attrRepo, attributeLookup,
                                frontEndQuery.getRatingModels(), timeTranslator, sqlUser));
                    } else {
                        queryBuilder.select(attributeLookup.getEntity(), attributeLookup.getAttribute());
                    }
                } else {
                    queryBuilder.select(lookup);
                }
            });
        }

        if (!hasRatingLookup.get() && !isCountQuery && frontEndQuery.getRatingModels() != null) {
            appendRuleLookups(frontEndQuery, attrRepo, queryBuilder, timeTranslator, sqlUser);
        }

        configurePagination(frontEndQuery);

        return queryBuilder.build();
    }

    private Restriction translateInnerRestriction(FrontEndQuery frontEndQuery, AttributeRepository attrRepo,
            BusinessEntity outerEntity, Restriction outerRestriction, Map<String, Lookup> ruleBasedModels,
            TimeFilterTranslator timeTranslator, String sqlUser) {
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
        Restriction innerRestriction = translateFrontEndRestriction(innerFrontEndRestriction, attrRepo, timeTranslator,
                sqlUser, true);
        translateRatingRuleRestriction(ruleBasedModels, innerRestriction);
        return addSubselectRestriction(outerEntity, outerRestriction, innerEntity, innerRestriction);
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

    private static void translateRuleBasedRating(Restriction restriction, Map<String, Lookup> ruleBasedModels) {
        if (MapUtils.isNotEmpty(ruleBasedModels)) {
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
                    // won't handle analytic models
                    log.warn("Cannot find the definition of rule based model " + modelId + ".");
                }
            }
        }
    }

    private Map<String, Lookup> ruleBasedModels(BusinessEntity mainEntity, AttributeRepository attrRepo,
            List<RatingModel> models, TimeFilterTranslator timeTranslator, String sqlUser) {
        Map<String, Lookup> lookupMap = new ConcurrentHashMap<>();
        if (models != null) {
            BusinessEntity entity = mainEntity != null ? mainEntity : BusinessEntity.Account;
            models.forEach(model -> {
                if (model instanceof RuleBasedModel) {
                    RatingRule ratingRule = ((RuleBasedModel) model).getRatingRule();
                    String modelId = model.getId();
                    Lookup lookup = translateRatingRule(entity, attrRepo, ratingRule, modelId, false, //
                            timeTranslator, sqlUser);
                    lookupMap.put(modelId, lookup);
                }
            });
        }
        return lookupMap;
    }

    private void appendRuleLookups(FrontEndQuery frontEndQuery, AttributeRepository attrRepo, QueryBuilder queryBuilder,
            TimeFilterTranslator timeTranslator, String sqlUser) {
        frontEndQuery.getRatingModels().forEach(model -> {
            if (model instanceof RuleBasedModel) {
                String alias = model.getId();
                if (frontEndQuery.getRatingModels().size() == 1) {
                    alias = "Score";
                }
                Lookup ruleLookup = translateRatingRule(frontEndQuery.getMainEntity(), attrRepo,
                        ((RuleBasedModel) model).getRatingRule(), alias, false, timeTranslator, sqlUser);
                queryBuilder.select(ruleLookup);
            } else {
                log.warn("Cannot not handle rating model of type " + model.getClass().getSimpleName());
            }
        });
    }

    private Lookup parseRatingLookup(BusinessEntity entity, AttributeRepository attrRepo, AttributeLookup lookup,
            List<RatingModel> models, TimeFilterTranslator timeTranslator, String sqlUser) {
        if (models == null) {
            throw new RuntimeException(
                    "You specified a rating lookup " + lookup + " but no rating models, cannot parse the lookup.");
        }
        RatingModel model = models.stream().filter(m -> lookup.getAttribute().equalsIgnoreCase(m.getId())).findFirst()
                .orElse(null);
        if (model != null) {
            if (models.get(0) instanceof RuleBasedModel) {
                RatingRule ratingRule = ((RuleBasedModel) models.get(0)).getRatingRule();
                return translateRatingRule(entity, attrRepo, ratingRule, lookup.getAttribute(), false, //
                        timeTranslator, sqlUser);
            } else {
                throw new UnsupportedOperationException("Only support rule based model now.");
            }
        } else {
            throw new RuntimeException("Cannot find a rating model with id=" + lookup.getAttribute());
        }
    }

    public Lookup translateRatingRule(BusinessEntity entity, AttributeRepository attrRepo, RatingRule ratingRule,
            String alias, boolean forScoreCount, TimeFilterTranslator timeTranslator, String sqlUser) {
        TreeMap<String, Restriction> cases = new TreeMap<>();
        AtomicInteger idx = new AtomicInteger(0);
        ratingRule.getBucketToRuleMap().forEach((key, val) -> {
            FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
            Restriction res = val.get(FrontEndQueryConstants.ACCOUNT_RESTRICTION);
            if (res != null) {
                res = res.getDeepCopy();
            }
            frontEndRestriction.setRestriction(res);
            // do not support nested ratings for now
            Restriction accountRestriction = translateFrontEndRestriction(frontEndRestriction, attrRepo, timeTranslator,
                    sqlUser, true);

            frontEndRestriction = new FrontEndRestriction();
            res = val.get(FrontEndQueryConstants.CONTACT_RESTRICTION);
            if (res != null) {
                res = res.getDeepCopy();
            }
            frontEndRestriction.setRestriction(res);
            // do not support nested ratings for now
            Restriction contactRestriction = translateFrontEndRestriction(frontEndRestriction, attrRepo, timeTranslator,
                    sqlUser, true);

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
                cases.put(key, addSubselectRestriction(entity, outerRestriction, innerEntity, innerRestriction));
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

}
