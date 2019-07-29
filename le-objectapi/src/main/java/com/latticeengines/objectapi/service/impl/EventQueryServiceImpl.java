package com.latticeengines.objectapi.service.impl;

import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
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
public class EventQueryServiceImpl extends BaseQueryServiceImpl implements EventQueryService {

    private static Logger log = LoggerFactory.getLogger(EventQueryServiceImpl.class);

    private static final String BATCH_USER = RedshiftQueryProvider.USER_BATCH;

    private final TransactionService transactionService;

    private String batchUserName;

    protected void setBatchUser(String batchUserName) {
        this.batchUserName = batchUserName;
        log.info("Set BatchUserName to {}", batchUserName);
    }

    protected String getBatchUser() {
        if (StringUtils.isNotBlank(batchUserName)) {
            return batchUserName;
        }
        return BATCH_USER;
    }

    @Inject
    public EventQueryServiceImpl(QueryEvaluatorService queryEvaluatorService, TransactionService transactionService) {
        super(queryEvaluatorService);
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

    /*
     * This query seems to be super complex and in some cases each query is as
     * big as 16 pages with different product and time periods selections by
     * user As this query is adding so much load on Leader, it is blocking all
     * other SEGMENT_USER queries. So, changed it back to BATCH_USER
     */
    private long getCount(CustomerSpace customerSpace, EventFrontEndQuery frontEndQuery, EventType eventType,
            DataCollection.Version version) {
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);
        try {
            Query query = getQuery(attrRepo, frontEndQuery, eventType);
            return queryEvaluatorService.getCount(attrRepo, query, getBatchUser());
        } catch (Exception e) {
            throw new QueryEvaluationException("Failed to execute query " + JsonUtils.serialize(frontEndQuery), e);
        }
    }

    private DataPage getData(CustomerSpace customerSpace, EventFrontEndQuery frontEndQuery, EventType eventType,
            DataCollection.Version version) {
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);
        try {
            Query query = getQuery(attrRepo, frontEndQuery, eventType);
            return queryEvaluatorService.getData(attrRepo, query, getBatchUser());
        } catch (Exception e) {
            throw new QueryEvaluationException("Failed to execute query " + JsonUtils.serialize(frontEndQuery), e);
        }
    }

    private Query getQuery(AttributeRepository attrRepo, EventFrontEndQuery frontEndQuery, EventType eventType) {
        ModelingQueryTranslator queryTranslator = new ModelingQueryTranslator(queryEvaluatorService.getQueryFactory(),
                attrRepo);
        TimeFilterTranslator timeTranslator = null;
        if (frontEndQuery.getSegmentQuery() != null) {
            timeTranslator = QueryServiceUtils.getTimeFilterTranslator(transactionService, //
                    frontEndQuery.getSegmentQuery());
            if (frontEndQuery.getMainEntity() == null) {
                frontEndQuery.setMainEntity(BusinessEntity.Account);
            }
            Map<ComparisonType, Set<AttributeLookup>> map = queryTranslator.needPreprocess(frontEndQuery, timeTranslator);
            if (frontEndQuery.getSegmentQuery() != null) {
                Map<ComparisonType, Set<AttributeLookup>> segmentMap = queryTranslator
                        .needPreprocess(frontEndQuery.getSegmentQuery(), timeTranslator);
                map.putAll(segmentMap);
            }
            preprocess(map, attrRepo, timeTranslator);
        }
        return queryTranslator.translateModelingEvent(frontEndQuery, eventType, timeTranslator, getBatchUser());
    }

    public String getQueryStr(EventFrontEndQuery frontEndQuery, EventType eventType, DataCollection.Version version, //
                              String sqlUser) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);
        Query query = getQuery(attrRepo, frontEndQuery, eventType);
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

    @Override
    public String getQueryStr(EventFrontEndQuery frontEndQuery, EventType eventType, DataCollection.Version version) {
        return getQueryStr(frontEndQuery, eventType, version, getBatchUser());
    }

}
