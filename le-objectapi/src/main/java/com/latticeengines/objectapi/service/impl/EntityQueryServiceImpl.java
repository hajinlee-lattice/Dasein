package com.latticeengines.objectapi.service.impl;

import java.util.Collections;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.EntityLookup;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.objectapi.util.QueryDecorator;
import com.latticeengines.objectapi.util.QueryTranslator;
import com.latticeengines.objectapi.util.AccountQueryDecorator;
import com.latticeengines.objectapi.util.ContactQueryDecorator;
import com.latticeengines.objectapi.service.EntityQueryService;
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
        Query query = QueryTranslator.translate(frontEndQuery, getDecorator(entity, false));
        query.setLookups(Collections.singletonList(new EntityLookup(entity)));
        return queryEvaluatorService.getCount(customerSpace.toString(), query);
    }

    @Override
    public DataPage getData(BusinessEntity entity, FrontEndQuery frontEndQuery) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Query query = QueryTranslator.translate(frontEndQuery, getDecorator(entity, true));
        if (query.getLookups() == null || query.getLookups().isEmpty()) {
            query.addLookup(new EntityLookup(entity));
        }
        return queryEvaluatorService.getData(customerSpace.toString(), query);
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
