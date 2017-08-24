package com.latticeengines.objectapi.service.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.query.exposed.evaluator.QueryEvaluator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.CaseLookup;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.EntityLookup;
import com.latticeengines.domain.exposed.query.GroupBy;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.objectapi.service.EntityQueryService;
import com.latticeengines.objectapi.util.AccountQueryDecorator;
import com.latticeengines.objectapi.util.ContactQueryDecorator;
import com.latticeengines.objectapi.util.QueryDecorator;
import com.latticeengines.objectapi.util.QueryTranslator;
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
    public long getCount(BusinessEntity entity, FrontEndQuery frontEndQuery) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Query query = QueryTranslator.translate(entity, frontEndQuery, getDecorator(entity, false));
        query.setLookups(Collections.singletonList(new EntityLookup(entity)));
        return queryEvaluatorService.getCount(customerSpace.toString(), query);
    }

    @Override
    public DataPage getData(BusinessEntity entity, FrontEndQuery frontEndQuery) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Query query = QueryTranslator.translate(entity, frontEndQuery, getDecorator(entity, true));
        if (query.getLookups() == null || query.getLookups().isEmpty()) {
            query.addLookup(new EntityLookup(entity));
        }
        return queryEvaluatorService.getData(customerSpace.toString(), query);
    }

    @Override
    public Map<String, Long> getRatingCount(BusinessEntity entity, FrontEndQuery frontEndQuery) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Query query = ratingCountQuery(entity, frontEndQuery);
        List<Map<String, Object>> data = queryEvaluatorService.getData(customerSpace.toString(), query).getData();
        Map<String, Long> counts = new HashMap<>();
        data.forEach(map -> counts.put((String) map.get(QueryEvaluator.SCORE), (Long) map.get("count")));
        return counts;
    }

    private Query ratingCountQuery(BusinessEntity entity, FrontEndQuery frontEndQuery) {
        List<RatingModel> models = frontEndQuery.getRatingModels();
        if (models != null && models.size() == 1) {
            Query query = QueryTranslator.translate(entity, frontEndQuery, getDecorator(entity, false));
            query.setPageFilter(null);
            RatingModel model = frontEndQuery.getRatingModels().get(0);
            if (model instanceof RuleBasedModel) {
                RuleBasedModel ruleBasedModel = (RuleBasedModel) model;
                CaseLookup caseLookup = QueryTranslator.translateRatingRule(entity, ruleBasedModel.getRatingRule(), QueryEvaluator.SCORE);
                GroupBy groupBy = new GroupBy();
                groupBy.setLookups(Collections.singletonList(caseLookup));
                query.setGroupBy(groupBy);
                query.addLookup(caseLookup);
                query.addLookup(AggregateLookup.count().as("Count"));
                return query;
            } else {
                throw new UnsupportedOperationException("Can not count rating model of type " + model.getClass().getSimpleName());
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
