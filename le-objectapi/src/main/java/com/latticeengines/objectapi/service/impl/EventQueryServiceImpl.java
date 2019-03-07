package com.latticeengines.objectapi.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.EventType;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.util.TimeFilterTranslator;
import com.latticeengines.objectapi.service.EventQueryService;
import com.latticeengines.objectapi.service.TransactionService;
import com.latticeengines.objectapi.util.ModelingQueryTranslator;
import com.latticeengines.objectapi.util.QueryServiceUtils;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.latticeengines.query.factory.RedshiftQueryProvider;

@Service("eventQueryService")
public class EventQueryServiceImpl implements EventQueryService {

    private static final String BATCH_USER = RedshiftQueryProvider.USER_BATCH;
    private static final String SEGMENT_USER = RedshiftQueryProvider.USER_SEGMENT;

    private final QueryEvaluatorService queryEvaluatorService;

    private final TransactionService transactionService;

    @Inject
    public EventQueryServiceImpl(QueryEvaluatorService queryEvaluatorService, TransactionService transactionService) {
        this.queryEvaluatorService = queryEvaluatorService;
        this.transactionService = transactionService;
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
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);
        try {
            ModelingQueryTranslator queryTranslator = new ModelingQueryTranslator(
                    queryEvaluatorService.getQueryFactory(), attrRepo);
            TimeFilterTranslator timeTranslator = QueryServiceUtils.getTimeFilterTranslator(transactionService,
                    frontEndQuery.getSegmentQuery());
            Query query = queryTranslator.translateModelingEvent(frontEndQuery, eventType, timeTranslator,
                    SEGMENT_USER);
            System.out.println("query is " + queryEvaluatorService.getQueryStr(attrRepo, query, SEGMENT_USER));
            return queryEvaluatorService.getCount(attrRepo, query, SEGMENT_USER);
        } catch (Exception e) {
            throw new QueryEvaluationException("Failed to execute query " + JsonUtils.serialize(frontEndQuery), e);
        }
    }

    private DataPage getData(CustomerSpace customerSpace, EventFrontEndQuery frontEndQuery, EventType eventType,
            DataCollection.Version version) {
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);
        try {
            ModelingQueryTranslator queryTranslator = new ModelingQueryTranslator(
                    queryEvaluatorService.getQueryFactory(), attrRepo);
            TimeFilterTranslator timeTranslator = QueryServiceUtils.getTimeFilterTranslator(transactionService,
                    frontEndQuery.getSegmentQuery());
            Query query = queryTranslator.translateModelingEvent(frontEndQuery, eventType, timeTranslator, BATCH_USER);
            return queryEvaluatorService.getData(attrRepo, query, BATCH_USER);
        } catch (Exception e) {
            throw new QueryEvaluationException("Failed to execute query " + JsonUtils.serialize(frontEndQuery), e);
        }
    }

}
