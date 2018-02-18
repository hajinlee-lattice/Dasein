package com.latticeengines.query.exposed.evaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.querydsl.sql.SQLQuery;

import reactor.core.publisher.Flux;

@Component("queryEvaluatorService")
public class QueryEvaluatorService {

    private static final Logger log = LoggerFactory.getLogger(QueryEvaluatorService.class);
    private static final int MAX_RETRIES = 2;

    @Autowired
    private DataCollectionProxy dataCollectionProxy; // attr repo cached in this proxy

    @Autowired
    private QueryEvaluator queryEvaluator;

    @Autowired
    private QueryFactory queryFactory;

    public AttributeRepository getAttributeRepository(String customerSpace, DataCollection.Version version) {
        return dataCollectionProxy.getAttrRepo(customerSpace, version);
    }

    public QueryFactory getQueryFactory() {
        return queryFactory;
    }

    public long getCount(AttributeRepository attrRepo, Query query) {
        long count = -1;
        if (query != null && query.getMainEntity() != null && query.getMainEntity().getServingStore() != null
                && attrRepo.getTableName(query.getMainEntity().getServingStore()) == null) {
            return 0;
        }
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        try (PerformanceTimer timer = new PerformanceTimer(timerMessage("fetchCount", attrRepo, sqlQuery))) {
            count = sqlQuery.fetchCount();
        }
        return count;
    }

    public DataPage getData(String customerSpace, DataCollection.Version version, Query query) {
        return getData(dataCollectionProxy.getAttrRepo(customerSpace, version), query);
    }

    public DataPage getData(AttributeRepository attrRepo, Query query) {
        DataPage dataPage = null;
        List<Lookup> filteredLookups = new ArrayList<>();
        for (Lookup lookup : query.getLookups()) {
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
            List<Map<String, Object>> results = queryEvaluator.pipe(sqlQuery, query).toStream() //
                    .collect(Collectors.toList());
            dataPage = new DataPage(results);
        }

        return dataPage;
    }

    public Flux<Map<String, Object>> getDataFlux(AttributeRepository attrRepo, Query query) {
        List<Lookup> filteredLookups = new ArrayList<>();
        for (Lookup lookup : query.getLookups()) {
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
            return queryEvaluator.pipe(sqlQuery, query);
        }
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
