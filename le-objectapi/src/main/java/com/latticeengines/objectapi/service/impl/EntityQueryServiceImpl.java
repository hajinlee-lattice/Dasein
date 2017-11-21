package com.latticeengines.objectapi.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
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
import com.latticeengines.objectapi.service.EntityQueryService;
import com.latticeengines.objectapi.util.AccountQueryDecorator;
import com.latticeengines.objectapi.util.ContactQueryDecorator;
import com.latticeengines.objectapi.util.ProductQueryDecorator;
import com.latticeengines.objectapi.util.QueryDecorator;
import com.latticeengines.objectapi.util.QueryServiceUtils;
import com.latticeengines.objectapi.util.QueryTranslator;
import com.latticeengines.query.exposed.evaluator.QueryEvaluator;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Service("entityQueryService")
public class EntityQueryServiceImpl implements EntityQueryService {

    private final QueryEvaluatorService queryEvaluatorService;

    @Autowired
    public EntityQueryServiceImpl(QueryEvaluatorService queryEvaluatorService) {
        this.queryEvaluatorService = queryEvaluatorService;
    }

    @Override
    public long getCount(FrontEndQuery frontEndQuery) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, queryEvaluatorService);
        QueryTranslator queryTranslator = new QueryTranslator(queryEvaluatorService.getQueryFactory(), attrRepo);
        Query query = queryTranslator.translate(frontEndQuery, getDecorator(frontEndQuery.getMainEntity(), false));
        query.setLookups(Collections.singletonList(new EntityLookup(frontEndQuery.getMainEntity())));
        return queryEvaluatorService.getCount(customerSpace.toString(), query);
    }

    @Override
    public DataPage getData(FrontEndQuery frontEndQuery) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, queryEvaluatorService);
        QueryTranslator queryTranslator = new QueryTranslator(queryEvaluatorService.getQueryFactory(), attrRepo);
        Query query = queryTranslator.translate(frontEndQuery, getDecorator(frontEndQuery.getMainEntity(), true));
        if (query.getLookups() == null || query.getLookups().isEmpty()) {
            query.addLookup(new EntityLookup(frontEndQuery.getMainEntity()));
        }
        query = preProcess(frontEndQuery.getMainEntity(), query);
        DataPage data = queryEvaluatorService.getData(customerSpace.toString(), query);
        return postProcess(frontEndQuery.getMainEntity(), data);
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

    private DataPage postProcess(BusinessEntity entity, DataPage data) {
        if (BusinessEntity.Contact == entity) {
            List<Map<String, Object>> results = data.getData();
            List<Map<String, Object>> processed = results.stream().map(objectMap -> {
                if (objectMap.containsKey(InterfaceName.CompanyName.toString())
                        && objectMap.containsKey(InterfaceName.LDC_Name.toString())) {
                    String companyName = (String) objectMap.get(InterfaceName.CompanyName.toString());
                    String ldcName = (String) objectMap.get(InterfaceName.LDC_Name.toString());
                    String consolidatedName = (ldcName != null) ? ldcName : companyName;
                    if (consolidatedName != null) {
                        objectMap.put(InterfaceName.CompanyName.toString(), consolidatedName);
                    }
                }
                return objectMap;
            }).collect(Collectors.toList());
            data.setData(processed);
        }
        return data;
    }

    @Override
    public Map<String, Long> getRatingCount(FrontEndQuery frontEndQuery) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Query query = ratingCountQuery(customerSpace, frontEndQuery);
        List<Map<String, Object>> data = queryEvaluatorService.getData(customerSpace.toString(), query).getData();
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
    }

    private Query ratingCountQuery(CustomerSpace customerSpace, FrontEndQuery frontEndQuery) {
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
            QueryTranslator queryTranslator = new QueryTranslator(
                    queryEvaluatorService.getQueryFactory(),
                    queryEvaluatorService.getAttributeRepository(customerSpace.toString()));
            Query query = queryTranslator.translate(frontEndQuery, getDecorator(frontEndQuery.getMainEntity(), false));
            query.setPageFilter(null);
            query.setSort(null);
            RatingModel model = frontEndQuery.getRatingModels().get(0);
            if (model instanceof RuleBasedModel) {
                RuleBasedModel ruleBasedModel = (RuleBasedModel) model;
                Lookup ruleLookup = queryTranslator.translateRatingRule(frontEndQuery.getMainEntity(),
                                                                        ruleBasedModel.getRatingRule(),
                                                                        QueryEvaluator.SCORE, true, null, false);
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

    private QueryDecorator getDecorator(BusinessEntity entity, boolean addSelects) {
        switch (entity) {
        case Account:
            return addSelects ? AccountQueryDecorator.WITH_SELECTS : AccountQueryDecorator.WITHOUT_SELECTS;
        case Contact:
            return addSelects ? ContactQueryDecorator.WITH_SELECTS : ContactQueryDecorator.WITHOUT_SELECTS;
        case Product:
            return addSelects ? ProductQueryDecorator.WITH_SELECTS : ProductQueryDecorator.WITHOUT_SELECTS;
        default:
            throw new UnsupportedOperationException("Cannot find a decorator for entity " + entity);
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
