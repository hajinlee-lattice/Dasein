package com.latticeengines.objectapi.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.EventType;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
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
    public DataPage getScoringTuples(EventFrontEndQuery frontEndQuery, DataCollection.Version version) {
        return getData(MultiTenantContext.getCustomerSpace(), frontEndQuery, EventType.Scoring, version);
    }

    @Override
    public DataPage getTrainingTuples(EventFrontEndQuery frontEndQuery, DataCollection.Version version) {
        return getData(MultiTenantContext.getCustomerSpace(), frontEndQuery, EventType.Training, version);
    }

    @Override
    public DataPage getEventTuples(EventFrontEndQuery frontEndQuery, DataCollection.Version version) {
        return getData(MultiTenantContext.getCustomerSpace(), frontEndQuery, EventType.Event, version);
    }

    @Override
    public long getScoringCount(EventFrontEndQuery frontEndQuery, DataCollection.Version version) {
        return getCount(MultiTenantContext.getCustomerSpace(), frontEndQuery, EventType.Scoring, version);
    }

    @Override
    public long getTrainingCount(EventFrontEndQuery frontEndQuery, DataCollection.Version version) {
        return getCount(MultiTenantContext.getCustomerSpace(), frontEndQuery, EventType.Training, version);
    }

    @Override
    public long getEventCount(EventFrontEndQuery frontEndQuery, DataCollection.Version version) {
        return getCount(MultiTenantContext.getCustomerSpace(), frontEndQuery, EventType.Event, version);
    }


    private long getCount(CustomerSpace customerSpace, EventFrontEndQuery frontEndQuery, EventType eventType,
                          DataCollection.Version version) {
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version, queryEvaluatorService);
        QueryTranslator queryTranslator = new QueryTranslator(queryEvaluatorService.getQueryFactory(), attrRepo);
        Query query = queryTranslator.translateModelingEvent(frontEndQuery, eventType);
        return queryEvaluatorService.getCount(attrRepo, query);
    }

    private DataPage getData(CustomerSpace customerSpace, EventFrontEndQuery frontEndQuery, EventType eventType,
                             DataCollection.Version version) {
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version, queryEvaluatorService);
        QueryTranslator queryTranslator = new QueryTranslator(queryEvaluatorService.getQueryFactory(), attrRepo);
        Query query = queryTranslator.translateModelingEvent(frontEndQuery, eventType);
        return queryEvaluatorService.getData(attrRepo, query);
    }

}
