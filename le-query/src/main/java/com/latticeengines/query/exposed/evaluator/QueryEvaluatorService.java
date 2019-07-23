package com.latticeengines.query.exposed.evaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.querydsl.sql.SQLQuery;

import reactor.core.publisher.Flux;

@Component("queryEvaluatorService")
public class QueryEvaluatorService {

    private static final Logger log = LoggerFactory.getLogger(QueryEvaluatorService.class);

    // attr repo cached in this proxy
    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private QueryEvaluator queryEvaluator;

    @Inject
    private QueryFactory queryFactory;

    public AttributeRepository getAttributeRepository(String customerSpace, DataCollection.Version version) {
        return dataCollectionProxy.getAttrRepo(customerSpace, version);
    }

    public QueryFactory getQueryFactory() {
        return queryFactory;
    }

    public long getCount(AttributeRepository attrRepo, Query query, String sqlUser) {
        query.setCount(true);
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        try (PerformanceTimer timer = new PerformanceTimer(timerMessage("fetchCount", attrRepo, sqlQuery))) {
            timer.setThreshold(0L);
            return sqlQuery.fetchCount();
        }
    }

    public String getQueryStr(AttributeRepository attrRepo, Query query, String sqlUser) {
        SQLQuery<?> sqlQuery = constructSqlQuery(attrRepo, query, sqlUser);
        sqlQuery.setUseLiterals(true);
        return sqlQuery.getSQL().getSQL();
    }

    public DataPage getData(String customerSpace, DataCollection.Version version, Query query, String sqlUser) {
        return getData(dataCollectionProxy.getAttrRepo(customerSpace, version), query, sqlUser);
    }

    public DataPage getData(AttributeRepository attrRepo, Query query, String sqlUser) {
        List<Map<String, Object>> results = getDataFlux(attrRepo, query, sqlUser).collectList().block();
        return new DataPage(results);
    }

    private SQLQuery<?> constructSqlQuery(AttributeRepository attrRepo, Query query, String sqlUser) {
        List<Lookup> filteredLookups = new ArrayList<>();
        for (Lookup lookup : query.getLookups()) {
            if (lookup instanceof AttributeLookup) {
                AttributeLookup attrLookup = (AttributeLookup) lookup;
                if (BusinessEntity.Rating.equals(attrLookup.getEntity()) //
                        || attrRepo.hasAttribute(attrLookup)) {
                    filteredLookups.add(lookup);
                } else {
                    log.warn("Cannot find metadata for attribute lookup " + lookup.toString() + ", skip it.");
                }
            } else {
                filteredLookups.add(lookup);
            }
        }
        query.setLookups(filteredLookups);
        return queryEvaluator.evaluate(attrRepo, query, sqlUser);
    }

    public Flux<Map<String, Object>> getDataFlux(AttributeRepository attrRepo, Query query, String sqlUser) {
        return getDataFlux(attrRepo, query, sqlUser, null);
    }

    public Flux<Map<String, Object>> getDataFlux(AttributeRepository attrRepo, Query query, String sqlUser, //
                                                 Map<String, Map<Long, String>> decodeMapping) {
        SQLQuery<?> sqlQuery = constructSqlQuery(attrRepo, query, sqlUser);
        AtomicLong startTime = new AtomicLong();
        AtomicLong counter = new AtomicLong(0);
        Flux<Map<String, Object>> flux = queryEvaluator.pipe(sqlQuery, query) //
                .doOnSubscribe(s -> startTime.set(System.currentTimeMillis())) //
                .doOnNext(m -> counter.getAndIncrement()) //
                .doOnComplete(() -> {
                    String msg = String.format(
                            "[Metric] Finished fetching %d records. tenantId=%s SQLQuery=%s ElapsedTime=%d ms",
                            counter.get(), attrRepo.getCustomerSpace().getTenantId(),
                            sqlQuery.getSQL().getSQL().trim().replaceAll(System.lineSeparator(), " "),
                            System.currentTimeMillis() - startTime.get());
                    log.info(msg);
                });
        if (MapUtils.isNotEmpty(decodeMapping)) {
            flux = flux.map(result -> postProcess(result, decodeMapping));
        }
        return flux;
    }

    private Map<String, Object> postProcess(Map<String, Object> result, Map<String, Map<Long, String>> decodeMapping) {
        if (MapUtils.isNotEmpty(result)) {
            final Map<String, Object> modifier = result;
            result.keySet() //
                    .stream() //
                    .filter(decodeMapping::containsKey) //
                    .forEach(key -> { //
                        Object val = modifier.get(key);
                        if (val instanceof Long) {
                            Long enumNumeric = (Long) val;
                            if (enumNumeric == 0L) { // 0 is null
                                modifier.put(key, null);
                            } else if (decodeMapping.get(key).containsKey(enumNumeric)) {
                                modifier.put(key, decodeMapping.get(key).get(enumNumeric));
                            } else {
                                modifier.put(key, enumNumeric);
                            }
                        }
                    });
        }
        return result;
    }

    private String timerMessage(String method, AttributeRepository attrRepo, SQLQuery<?> sqlQuery) {
        return String.format("%s tenantId=%s SQLQuery=%s", method, attrRepo.getCustomerSpace().getTenantId(),
                sqlQuery.getSQL().getSQL().trim().replaceAll(System.lineSeparator(), " "));
    }

}
