package com.latticeengines.objectapi.service.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.CaseLookup;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.EntityLookup;
import com.latticeengines.domain.exposed.query.GroupBy;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.objectapi.service.EntityQueryService;
import com.latticeengines.objectapi.util.AccountQueryDecorator;
import com.latticeengines.objectapi.util.ContactQueryDecorator;
import com.latticeengines.objectapi.util.QueryDecorator;
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
        Query query = QueryTranslator.translate(frontEndQuery, getDecorator(frontEndQuery.getMainEntity(), false));
        query.setLookups(Collections.singletonList(new EntityLookup(frontEndQuery.getMainEntity())));
        return queryEvaluatorService.getCount(customerSpace.toString(), query);
    }

    @Override
    public DataPage getData(FrontEndQuery frontEndQuery) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Query query = QueryTranslator.translate(frontEndQuery, getDecorator(frontEndQuery.getMainEntity(), true));
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
                List<Lookup> filtered =
                    lookups.stream().filter(this::isContactCompanyNameLookup).collect(Collectors.toList());
                filtered.add(new AttributeLookup(BusinessEntity.Account, InterfaceName.CompanyName.toString()));
                filtered.add(new AttributeLookup(BusinessEntity.Account, InterfaceName.LDC_Name.toString()));
                query.setLookups(filtered);
            }
        }
        return query;
    }

    private boolean isContactCompanyNameLookup(Lookup lookup) {
        if (lookup instanceof AttributeLookup) {
            AttributeLookup attrLookup = (AttributeLookup) lookup;
            String attributeName = attrLookup.getAttribute();
            if (attributeName.equals(InterfaceName.CompanyName.toString()) &&
                BusinessEntity.Contact == attrLookup.getEntity()) {
                return true;
            }
        }
        return false;
    }

    private DataPage postProcess(BusinessEntity entity, DataPage data) {
        if (BusinessEntity.Contact == entity) {
            List<Map<String, Object>> results = data.getData();
            List<Map<String, Object>> processed = results.stream().map(objectMap -> {
                if (objectMap.containsKey(InterfaceName.CompanyName.toString()) &&
                    objectMap.containsKey(InterfaceName.LDC_Name.toString())) {
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
        Query query = ratingCountQuery(frontEndQuery);
        List<Map<String, Object>> data = queryEvaluatorService.getData(customerSpace.toString(), query).getData();
        Map<String, Long> counts = new HashMap<>();
        data.forEach(map -> counts.put((String) map.get(QueryEvaluator.SCORE), (Long) map.get("count")));
        return counts;
    }

    private Query ratingCountQuery(FrontEndQuery frontEndQuery) {
        List<RatingModel> models = frontEndQuery.getRatingModels();
        if (models != null && models.size() == 1) {
            Query query = QueryTranslator.translate(frontEndQuery, getDecorator(frontEndQuery.getMainEntity(), false));
            query.setPageFilter(null);
            RatingModel model = frontEndQuery.getRatingModels().get(0);
            if (model instanceof RuleBasedModel) {
                RuleBasedModel ruleBasedModel = (RuleBasedModel) model;
                CaseLookup caseLookup = QueryTranslator.translateRatingRule(frontEndQuery.getMainEntity(),
                        ruleBasedModel.getRatingRule(),
                        QueryEvaluator.SCORE);
                GroupBy groupBy = new GroupBy();
                groupBy.setLookups(Collections.singletonList(caseLookup));
                query.setGroupBy(groupBy);
                query.addLookup(caseLookup);
                query.addLookup(AggregateLookup.count().as("Count"));
                return query;
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
        default:
            throw new UnsupportedOperationException("Cannot find a decorator for entity " + entity);
        }
    }

}
