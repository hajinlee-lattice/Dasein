package com.latticeengines.objectapi.util;

import java.util.List;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.graph.traversal.impl.BreadthFirstSearch;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.CaseLookup;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.QueryBuilder;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.Sort;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.domain.exposed.util.RestrictionOptimizer;

public class QueryTranslator {
    private static final Logger log = LoggerFactory.getLogger(QueryTranslator.class);

    public static final int MAX_ROWS = 250;
    private static final PageFilter DEFAULT_PAGE_FILTER = new PageFilter(0, 100);

    public static Query translate(FrontEndQuery frontEndQuery, QueryDecorator decorator) {
        Restriction restriction = translateFrontEndRestriction(frontEndQuery.getAccountRestriction());
        if (frontEndQuery.restrictNullSalesforceId()) {
            Restriction sfidRestriction = Restriction.builder().let(BusinessEntity.Account, "SalesforceAccountID")
                    .isNull().build();
            restriction = Restriction.builder().and(restriction, sfidRestriction).build();
        } else if (frontEndQuery.restrictNotNullSalesforceId()) {
            Restriction sfidRestriction = Restriction.builder().let(BusinessEntity.Account, "SalesforceAccountID")
                    .isNotNull().build();
            restriction = Restriction.builder().and(restriction, sfidRestriction).build();
        }

        if (frontEndQuery.getPageFilter() == null) {
            frontEndQuery.setPageFilter(DEFAULT_PAGE_FILTER);
        } else {
            if (frontEndQuery.getPageFilter().getNumRows() > MAX_ROWS) {
                log.warn(String.format("Refusing to accept a query requesting more than %s rows."
                        + " Currently specified page filter: %s", MAX_ROWS, frontEndQuery.getPageFilter()));
                frontEndQuery.getPageFilter().setNumRows(MAX_ROWS);
            }
        }

        QueryBuilder queryBuilder = Query.builder().where(restriction) //
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
                queryBuilder.select(decorator.getLookupEntity(), decorator.getEntityLookups());
                if (frontEndQuery.getRatingModels() != null) {
                    frontEndQuery.getRatingModels().forEach(model -> {
                        if (model instanceof RuleBasedModel) {
                            String alias = model.getId();
                            if (frontEndQuery.getRatingModels().size() == 1) {
                                alias = "Score";
                            }
                            CaseLookup caseLookup = translateRatingRule(frontEndQuery.getMainEntity(),
                                    ((RuleBasedModel) model).getRatingRule(), alias);
                            queryBuilder.select(caseLookup);
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

    private static Restriction translateFrontEndRestriction(FrontEndRestriction frontEndRestriction) {
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
                    Restriction concrete = bucket.convert();
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    parent.getRestrictions().remove(bucket);
                    parent.getRestrictions().add(concrete);
                }
            });
            translated = restriction;
        } else if (restriction instanceof BucketRestriction) {
            BucketRestriction bucket = (BucketRestriction) restriction;
            translated = bucket.convert();
        } else {
            translated = restriction;
        }
        return RestrictionOptimizer.optimize(translated);
    }

    private static Sort translateFrontEndSort(FrontEndSort frontEndSort) {
        if (frontEndSort != null) {
            return new Sort(frontEndSort.getAttributes(), frontEndSort.getDescending());
        } else {
            return null;
        }
    }

    public static CaseLookup translateRatingRule(BusinessEntity entity, RatingRule ratingRule, String alias) {
        // TODO: only handles account restriction now
        TreeMap<String, Restriction> cases = new TreeMap<>();
        ratingRule.getBucketToRuleMap().forEach((key, val) -> {
            FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
            frontEndRestriction.setRestriction(val.get(RatingRule.ACCOUNT_RULE));
            cases.put(key, translateFrontEndRestriction(frontEndRestriction));
        });
        return new CaseLookup(cases, ratingRule.getDefaultBucketName(), alias);
    }

    private static CaseLookup parseRatingLookup(BusinessEntity entity, AttributeLookup lookup, List<RatingModel> models) {
        if (models == null) {
            throw new RuntimeException("You specified a rating lookup " + lookup + " but no rating models, cannot parse the lookup.");
        }
        RatingModel model = models.stream().filter(m -> lookup.getAttribute().equalsIgnoreCase(m.getId())).findFirst().orElse(null);
        if (model != null) {
            if (models.get(0) instanceof RuleBasedModel) {
                RatingRule ratingRule = ((RuleBasedModel) models.get(0)).getRatingRule();
                return translateRatingRule(entity, ratingRule, lookup.getAttribute());
            } else {
                throw new UnsupportedOperationException("Only support rule based model now.");
            }
        } else {
            throw new RuntimeException("Cannot find a rating model with id=" + lookup.getAttribute());
        }
    }

}
