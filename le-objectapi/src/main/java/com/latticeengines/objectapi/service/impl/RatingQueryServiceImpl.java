package com.latticeengines.objectapi.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.EntityLookup;
import com.latticeengines.domain.exposed.query.GroupBy;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.util.TimeFilterTranslator;
import com.latticeengines.objectapi.service.RatingQueryService;
import com.latticeengines.objectapi.service.TransactionService;
import com.latticeengines.objectapi.util.QueryServiceUtils;
import com.latticeengines.objectapi.util.RatingQueryTranslator;
import com.latticeengines.query.exposed.evaluator.QueryEvaluator;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;

import reactor.core.publisher.Flux;

@Service("ratingQueryService")
public class RatingQueryServiceImpl extends BaseQueryServiceImpl implements RatingQueryService {

    private final TransactionService transactionService;

    @Inject
    public RatingQueryServiceImpl(QueryEvaluatorService queryEvaluatorService, TransactionService transactionService) {
        super(queryEvaluatorService);
        this.transactionService = transactionService;
    }

    @Override
    public long getCount(FrontEndQuery frontEndQuery, DataCollection.Version version, String sqlUser) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);
        try {
            RatingQueryTranslator queryTranslator = new RatingQueryTranslator(queryEvaluatorService.getQueryFactory(),
                    attrRepo);
            TimeFilterTranslator timeTranslator = getTimeFilterTranslator(frontEndQuery);
            Map<ComparisonType, Set<AttributeLookup>> map = queryTranslator.needPreprocess(frontEndQuery,
                    timeTranslator);
            preprocess(map, attrRepo, timeTranslator);
            Query query = queryTranslator.translateRatingQuery(frontEndQuery, true, timeTranslator, sqlUser);
            query.setLookups(Collections.singletonList(new EntityLookup(frontEndQuery.getMainEntity())));
            return queryEvaluatorService.getCount(attrRepo, query, sqlUser);
        } catch (Exception e) {
            String msg = "Failed to execute query " + JsonUtils.serialize(frontEndQuery) //
                    + " for tenant " + MultiTenantContext.getShortTenantId();
            if (version != null) {
                msg += " in " + version;
            }
            throw new QueryEvaluationException(msg, e);
        }
    }

    @Override
    public DataPage getData(FrontEndQuery frontEndQuery, DataCollection.Version version, String sqlUser) {
        Flux<Map<String, Object>> flux = getDataFlux(frontEndQuery, version, sqlUser);
        List<Map<String, Object>> data = flux.collectList().block();
        return new DataPage(data);
    }

    @Override
    public String getQueryStr(FrontEndQuery frontEndQuery, DataCollection.Version version, String sqlUser) {
        Query query = getDataQuery(frontEndQuery, version, sqlUser);
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);
        try {
            return queryEvaluatorService.getQueryStr(attrRepo, query, sqlUser);
        } catch (Exception e) {
            String msg = "Failed to construct query string " + JsonUtils.serialize(frontEndQuery) //
                    + " for tenant " + MultiTenantContext.getShortTenantId();
            if (version != null) {
                msg += " in " + version;
            }
            throw new QueryEvaluationException(msg, e);
        }
    }

    private Query getDataQuery(FrontEndQuery frontEndQuery, DataCollection.Version version, String sqlUser) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);
        RatingQueryTranslator queryTranslator = new RatingQueryTranslator(queryEvaluatorService.getQueryFactory(),
                attrRepo);
        TimeFilterTranslator timeTranslator = getTimeFilterTranslator(frontEndQuery);
        Map<ComparisonType, Set<AttributeLookup>> map = queryTranslator.needPreprocess(frontEndQuery, timeTranslator);
        preprocess(map, attrRepo, timeTranslator);
        Query query = queryTranslator.translateRatingQuery(frontEndQuery, false, timeTranslator, sqlUser);
        if (query.getLookups() == null || query.getLookups().isEmpty()) {
            query.addLookup(new EntityLookup(frontEndQuery.getMainEntity()));
        }
        return query;
    }

    private Flux<Map<String, Object>> getDataFlux(FrontEndQuery frontEndQuery, DataCollection.Version version,
            String sqlUser) {
        Query query = getDataQuery(frontEndQuery, version, sqlUser);
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);
        try {
            return queryEvaluatorService.getDataFlux(attrRepo, query, sqlUser);
        } catch (Exception e) {
            String msg = "Failed to execute query " + JsonUtils.serialize(frontEndQuery) //
                    + " for tenant " + MultiTenantContext.getShortTenantId();
            if (version != null) {
                msg += " in " + version;
            }
            throw new QueryEvaluationException(msg, e);
        }
    }

    @Override
    public Map<String, Long> getRatingCount(FrontEndQuery frontEndQuery, DataCollection.Version version,
            String sqlUser) {
        try {
            CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
            Query query = ratingCountQuery(customerSpace, frontEndQuery, version, sqlUser);
            List<Map<String, Object>> data = queryEvaluatorService
                    .getData(customerSpace.toString(), version, query, sqlUser).getData();
            RatingModel model = frontEndQuery.getRatingModels().get(0);
            Map<String, String> lblMap = ruleLabelReverseMapping(((RuleBasedModel) model).getRatingRule());
            TreeMap<String, Long> counts = new TreeMap<>();
            data.forEach(map -> {
                String key = lblMap.get(map.get(QueryEvaluator.SCORE));
                if (!counts.containsKey(key)) {
                    counts.put(key, 0L);
                }
                counts.put(key, counts.get(key) + (Long) map.get("count"));
            });
            return counts;
        } catch (Exception e) {
            String msg = "Failed to execute query " + JsonUtils.serialize(frontEndQuery) //
                    + " for tenant " + MultiTenantContext.getShortTenantId();
            if (version != null) {
                msg += " in " + version;
            }
            throw new QueryEvaluationException(msg, e);
        }
    }

    private Query ratingCountQuery(CustomerSpace customerSpace, FrontEndQuery frontEndQuery,
            DataCollection.Version version, String sqlUser) {
        List<RatingModel> models = frontEndQuery.getRatingModels();
        if (models != null && models.size() == 1) {
            Restriction accountRestriction = frontEndQuery.getAccountRestriction() == null ? null
                    : frontEndQuery.getAccountRestriction().getRestriction();
            Restriction contactRestriction = frontEndQuery.getContactRestriction() == null ? null
                    : frontEndQuery.getContactRestriction().getRestriction();
            if (contactRestriction != null) {
                // merge account and contact restrictions
                accountRestriction = accountRestriction == null ? contactRestriction
                        : Restriction.builder().and(accountRestriction, contactRestriction).build();
                frontEndQuery.setAccountRestriction(new FrontEndRestriction(accountRestriction));
                frontEndQuery.setContactRestriction(null);
            }
            RatingQueryTranslator queryTranslator = new RatingQueryTranslator(queryEvaluatorService.getQueryFactory(),
                    queryEvaluatorService.getAttributeRepository(customerSpace.toString(), version));
            TimeFilterTranslator timeTranslator = getTimeFilterTranslator(frontEndQuery);
            Map<ComparisonType, Set<AttributeLookup>> map = queryTranslator.needPreprocess(frontEndQuery,
                    timeTranslator);
            AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                    queryEvaluatorService);
            preprocess(map, attrRepo, timeTranslator);
            Query query = queryTranslator.translateRatingQuery(frontEndQuery, true, timeTranslator, sqlUser);
            query.setPageFilter(null);
            query.setSort(null);
            RatingModel model = frontEndQuery.getRatingModels().get(0);
            if (model instanceof RuleBasedModel) {
                RuleBasedModel ruleBasedModel = (RuleBasedModel) model;
                Lookup ruleLookup = queryTranslator.translateRatingRule(frontEndQuery.getMainEntity(), //
                        ruleBasedModel.getRatingRule(), QueryEvaluator.SCORE, true, //
                        timeTranslator, sqlUser);
                AttributeLookup idLookup = new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name());
                query.setLookups(Arrays.asList(idLookup, ruleLookup));
                GroupBy groupBy = new GroupBy();
                groupBy.setLookups(Collections.singletonList(idLookup));
                query.setGroupBy(groupBy);

                SubQuery subQuery = new SubQuery(query, "q");
                SubQueryAttrLookup subQueryAttrLookup = new SubQueryAttrLookup(subQuery, QueryEvaluator.SCORE);
                return Query.builder() //
                        .select(subQueryAttrLookup, AggregateLookup.count().as("Count")) //
                        .from(subQuery) //
                        .groupBy(subQueryAttrLookup) //
                        .build();
            } else {
                throw new UnsupportedOperationException(
                        "Can not count rating model of type " + model.getClass().getSimpleName());
            }
        } else {
            throw new RuntimeException("Must specify one and only one rating model.");
        }
    }

    private TimeFilterTranslator getTimeFilterTranslator(FrontEndQuery frontEndQuery) {
        if (transactionService.needTimeFilterTranslator(frontEndQuery)) {
            return transactionService.getTimeFilterTranslator(frontEndQuery.getEvaluationDateStr());
        } else {
            return null;
        }
    }

    private static Map<String, String> ruleLabelReverseMapping(RatingRule ratingRule) {
        Map<String, String> lblMap = new HashMap<>();
        AtomicInteger idx = new AtomicInteger(0);
        ratingRule.getBucketToRuleMap().forEach((key, val) -> lblMap.put(String.valueOf(idx.getAndIncrement()), key));
        lblMap.put(String.valueOf(idx.get()), ratingRule.getDefaultBucketName());
        return lblMap;
    }

}
