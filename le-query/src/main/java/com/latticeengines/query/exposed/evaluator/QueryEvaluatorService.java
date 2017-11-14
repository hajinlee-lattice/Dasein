package com.latticeengines.query.exposed.evaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.querydsl.sql.SQLQuery;

@Component("queryEvaluatorService")
public class QueryEvaluatorService {

    private static final Logger log = LoggerFactory.getLogger(QueryEvaluatorService.class);

    @Autowired
    private DataCollectionProxy dataCollectionProxy; // attr repo cached in this proxy

    @Autowired
    private QueryEvaluator queryEvaluator;

    @Autowired
    private QueryFactory queryFactory;

    public AttributeRepository getAttributeRepository(String customerSpace) {
        return dataCollectionProxy.getAttrRepo(customerSpace);
    }

    public QueryFactory getQueryFactory() {
        return queryFactory;
    }

    public long getCount(String customerSpace, Query query) {
        return getCount(dataCollectionProxy.getAttrRepo(customerSpace), query);
    }

    public long getCount(AttributeRepository attrRepo, Query query) {
        long count = -1;
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        try (PerformanceTimer timer = new PerformanceTimer(timerMessage("fetchCount", attrRepo, sqlQuery))) {
            count = sqlQuery.fetchCount();
        }
        return count;
    }

    public DataPage getData(String customerSpace, Query query) {
        return getData(dataCollectionProxy.getAttrRepo(customerSpace), query);
    }

    public DataPage getData(AttributeRepository attrRepo, Query query) {
        DataPage dataPage = null;
        List<Lookup> filteredLookups = new ArrayList<>();
        for (Lookup lookup: query.getLookups()) {
            if (lookup instanceof AttributeLookup) {
                AttributeLookup attrLookup = (AttributeLookup) lookup;
                if (BusinessEntity.Rating.equals(attrLookup.getEntity()) || attrRepo.hasAttribute(attrLookup)) {
                    filteredLookups.add(lookup);
                } else {
                    log.warn("Cannot find metadata for attribute lookup " + lookup.toString() + ", skip it.");
                }
            } else {
                filteredLookups.add(lookup);
            }
        }
        query.setLookups(filteredLookups);
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        try (PerformanceTimer timer = new PerformanceTimer(timerMessage("getData", attrRepo, sqlQuery))) {
            List<Map<String, Object>> results = queryEvaluator.run(sqlQuery, attrRepo, query).getData();
            dataPage = new DataPage(results);
        }

        return dataPage;
    }

    private String timerMessage(String method, AttributeRepository attrRepo, SQLQuery<?> sqlQuery) {
        sqlQuery.getSQL().getSQL();
        return String.format("%s tenantId=%s SQLQuery=%s", method, attrRepo.getCustomerSpace().getTenantId(),
                sqlQuery.getSQL().getSQL().trim().replaceAll(System.lineSeparator(), " "));
    }

    public void setDataCollectionProxy(DataCollectionProxy dataCollectionProxy) {
        this.dataCollectionProxy = dataCollectionProxy;
    }
}
