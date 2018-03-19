package com.latticeengines.objectapi.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import com.latticeengines.objectapi.util.AccountQueryDecorator;
import com.latticeengines.objectapi.util.ContactQueryDecorator;
import com.latticeengines.objectapi.util.ProductQueryDecorator;
import com.latticeengines.objectapi.util.QueryDecorator;
import com.latticeengines.objectapi.util.QueryServiceUtils;
import com.latticeengines.objectapi.util.RatingQueryTranslator;
import com.latticeengines.query.exposed.evaluator.QueryEvaluator;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;

import reactor.core.publisher.Flux;


@Service("ratingQueryService")
public class RatingQueryServiceImpl implements RatingQueryService {

    private static final Logger log = LoggerFactory.getLogger(RatingQueryServiceImpl.class);

    private final QueryEvaluatorService queryEvaluatorService;

    private final TransactionService transactionService;

    @Inject
    public RatingQueryServiceImpl(QueryEvaluatorService queryEvaluatorService, TransactionService transactionService) {
        this.queryEvaluatorService = queryEvaluatorService;
        this.transactionService = transactionService;
    }

    @Override
    public long getCount(FrontEndQuery frontEndQuery, DataCollection.Version version) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);
        try {
            RatingQueryTranslator queryTranslator = new RatingQueryTranslator(queryEvaluatorService.getQueryFactory(),
                    attrRepo);
            QueryDecorator decorator = getDecorator(frontEndQuery.getMainEntity(), false);
            TimeFilterTranslator timeTranslator = getTimeFilterTranslator(frontEndQuery);
            Query query = queryTranslator.translateRatingQuery(frontEndQuery, decorator, timeTranslator);
            query.setLookups(Collections.singletonList(new EntityLookup(frontEndQuery.getMainEntity())));
            return queryEvaluatorService.getCount(attrRepo, query);
        } catch (Exception e) {
            throw new QueryEvaluationException("Failed to execute query " + JsonUtils.serialize(frontEndQuery), e);
        }
    }

    @Override
    public DataPage getData(FrontEndQuery frontEndQuery, DataCollection.Version version) {
        Flux<Map<String, Object>> flux = getDataFlux(frontEndQuery, version);
        List<Map<String, Object>> data = flux.collectList().block();
        return new DataPage(data);
    }

    private Flux<Map<String, Object>> getDataFlux(FrontEndQuery frontEndQuery, DataCollection.Version version) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);
        try {
            RatingQueryTranslator queryTranslator = new RatingQueryTranslator(queryEvaluatorService.getQueryFactory(),
                    attrRepo);
            QueryDecorator decorator = getDecorator(frontEndQuery.getMainEntity(), true);
            TimeFilterTranslator timeTranslator = getTimeFilterTranslator(frontEndQuery);
            Query query = queryTranslator.translateRatingQuery(frontEndQuery, decorator, timeTranslator);
            if (query.getLookups() == null || query.getLookups().isEmpty()) {
                query.addLookup(new EntityLookup(frontEndQuery.getMainEntity()));
            }
            query = preProcess(frontEndQuery.getMainEntity(), query);
            return queryEvaluatorService.getDataFlux(attrRepo, query) //
                    .map(row -> postProcess(frontEndQuery.getMainEntity(), row));
        } catch (Exception e) {
            throw new QueryEvaluationException("Failed to execute query " + JsonUtils.serialize(frontEndQuery), e);
        }
    }

    @Override
    public Map<String, Long> getRatingCount(FrontEndQuery frontEndQuery, DataCollection.Version version) {
        try {
            CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
            Query query = ratingCountQuery(customerSpace, frontEndQuery, version);
            List<Map<String, Object>> data = queryEvaluatorService.getData(customerSpace.toString(), version, query)
                    .getData();
            RatingModel model = frontEndQuery.getRatingModels().get(0);
            Map<String, String> lblMap = ruleLabelReverseMapping(((RuleBasedModel) model).getRatingRule());
            TreeMap<String, Long> counts = new TreeMap<>();
            data.forEach(map -> {
                String key = lblMap.get((String) map.get(QueryEvaluator.SCORE));
                if (!counts.containsKey(key)) {
                    counts.put(key, 0L);
                }
                counts.put(key, counts.get(key) + (Long) map.get("count"));
            });
            return counts;
        } catch (Exception e) {
            throw new QueryEvaluationException("Failed to execute query " + JsonUtils.serialize(frontEndQuery), e);
        }
    }

    private Query ratingCountQuery(CustomerSpace customerSpace, FrontEndQuery frontEndQuery,
                                   DataCollection.Version version) {
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
            Query query = queryTranslator.translateRatingQuery(frontEndQuery, null, timeTranslator);
            query.setPageFilter(null);
            query.setSort(null);
            RatingModel model = frontEndQuery.getRatingModels().get(0);
            if (model instanceof RuleBasedModel) {
                RuleBasedModel ruleBasedModel = (RuleBasedModel) model;
                Lookup ruleLookup = queryTranslator.translateRatingRule(frontEndQuery.getMainEntity(),
                        ruleBasedModel.getRatingRule(), QueryEvaluator.SCORE, true, null, timeTranslator);
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
        if (transactionService.hasTransactionBucket(frontEndQuery)) {
            return transactionService.getTimeFilterTranslator(frontEndQuery.getEvaluationDateStr());
        } else {
            return null;
        }
    }

    private Query preProcess(BusinessEntity entity, Query query) {
        if (BusinessEntity.Contact == entity) {
            List<Lookup> lookups = query.getLookups();
            if (lookups != null && lookups.stream().anyMatch(this::isContactCompanyNameLookup)) {
                List<Lookup> filtered = lookups.stream().filter(this::notContactCompanyNameLookup)
                        .collect(Collectors.toList());
                filtered.add(new AttributeLookup(BusinessEntity.Account, InterfaceName.CompanyName.toString()));
                filtered.add(new AttributeLookup(BusinessEntity.Account, InterfaceName.LDC_Name.toString()));
                query.setLookups(filtered);
            }
        }
        return query;
    }

    private boolean notContactCompanyNameLookup(Lookup lookup) {
        return !isContactCompanyNameLookup(lookup);
    }

    private boolean isContactCompanyNameLookup(Lookup lookup) {
        if (lookup instanceof AttributeLookup) {
            AttributeLookup attrLookup = (AttributeLookup) lookup;
            String attributeName = attrLookup.getAttribute();
            if (attributeName.equals(InterfaceName.CompanyName.toString())
                    && BusinessEntity.Contact == attrLookup.getEntity()) {
                return true;
            }
        }
        return false;
    }

    private Map<String, Object> postProcess(BusinessEntity entity, Map<String, Object> result) {
        Map<String, Object> processed = result;
        if (BusinessEntity.Account.equals(entity) || BusinessEntity.Contact.equals(entity)) {
            if (result.containsKey(InterfaceName.CompanyName.toString())
                    && result.containsKey(InterfaceName.LDC_Name.toString())) {
                processed = new HashMap<>();
                result.forEach(processed::put);
                String companyName = (String) processed.get(InterfaceName.CompanyName.toString());
                String ldcName = (String) processed.get(InterfaceName.LDC_Name.toString());
                String consolidatedName = (ldcName != null) ? ldcName : companyName;
                if (consolidatedName != null) {
                    processed.put(InterfaceName.CompanyName.toString(), consolidatedName);
                }
            }
        }
        return processed;
    }

    private QueryDecorator getDecorator(BusinessEntity entity, boolean addSelects) {
        switch (entity) {
            case Account:
                return addSelects ? AccountQueryDecorator.WITH_SELECTS : AccountQueryDecorator.WITHOUT_SELECTS;
            case Contact:
                return addSelects ? ContactQueryDecorator.WITH_SELECTS : ContactQueryDecorator.WITHOUT_SELECTS;
            case Product:
                return addSelects ? ProductQueryDecorator.WITH_SELECTS : ProductQueryDecorator.WITHOUT_SELECTS;
            default:
                log.warn("Cannot find a decorator for entity " + entity);
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
