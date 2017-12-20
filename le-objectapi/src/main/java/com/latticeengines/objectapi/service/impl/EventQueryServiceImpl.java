package com.latticeengines.objectapi.service.impl;

import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.EventType;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.objectapi.service.EventQueryService;
import com.latticeengines.objectapi.util.QueryServiceUtils;
import com.latticeengines.objectapi.util.QueryTranslator;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Service("eventQueryService")
public class EventQueryServiceImpl implements EventQueryService {

    private final QueryEvaluatorService queryEvaluatorService;

    @Autowired
    public EventQueryServiceImpl(QueryEvaluatorService queryEvaluatorService) {
        this.queryEvaluatorService = queryEvaluatorService;
    }

    @Override
    public DataPage getScoringTuples(EventFrontEndQuery frontEndQuery) {
        return getData(MultiTenantContext.getCustomerSpace(), frontEndQuery, EventType.Scoring);
    }

    @Override
    public DataPage getTrainingTuples(EventFrontEndQuery frontEndQuery) {
        return getData(MultiTenantContext.getCustomerSpace(), frontEndQuery, EventType.Training);
    }

    @Override
    public DataPage getEventTuples(EventFrontEndQuery frontEndQuery) {
        return getData(MultiTenantContext.getCustomerSpace(), frontEndQuery, EventType.Event);
    }

    @Override
    public long getScoringCount(EventFrontEndQuery frontEndQuery) {
        return getCount(MultiTenantContext.getCustomerSpace(), frontEndQuery, EventType.Scoring);
    }

    @Override
    public long getTrainingCount(EventFrontEndQuery frontEndQuery) {
        return getCount(MultiTenantContext.getCustomerSpace(), frontEndQuery, EventType.Training);
    }

    @Override
    public long getEventCount(EventFrontEndQuery frontEndQuery) {
        return getCount(MultiTenantContext.getCustomerSpace(), frontEndQuery, EventType.Event);
    }


    private long getCount(CustomerSpace customerSpace, EventFrontEndQuery frontEndQuery, EventType eventType) {
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, queryEvaluatorService);
        QueryTranslator queryTranslator = new QueryTranslator(queryEvaluatorService.getQueryFactory(), attrRepo);
        Query query = queryTranslator.translateModelingEvent(frontEndQuery, eventType);
        return queryEvaluatorService.getCount(customerSpace.toString(), query);
    }

    private DataPage getData(CustomerSpace customerSpace, EventFrontEndQuery frontEndQuery, EventType eventType) {
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, queryEvaluatorService);
        QueryTranslator queryTranslator = new QueryTranslator(queryEvaluatorService.getQueryFactory(), attrRepo);
        Query query = queryTranslator.translateModelingEvent(frontEndQuery, eventType);
        return queryEvaluatorService.getData(customerSpace.toString(), query);
    }

}
