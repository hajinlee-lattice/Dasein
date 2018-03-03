package com.latticeengines.query.exposed.evaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    @Inject
    private DataCollectionProxy dataCollectionProxy; // attr repo cached in this proxy

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

    public long getCount(AttributeRepository attrRepo, Query query) {
        long count;
        if (query != null && query.getMainEntity() != null && query.getMainEntity().getServingStore() != null
                && attrRepo.getTableName(query.getMainEntity().getServingStore()) == null) {
            count = 0;
        } else {
            SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query);
            try (PerformanceTimer timer = new PerformanceTimer(timerMessage("fetchCount", attrRepo, sqlQuery))) {
                count = sqlQuery.fetchCount();
            }
        }
        return count;
    }

    public DataPage getData(String customerSpace, DataCollection.Version version, Query query) {
        return getData(dataCollectionProxy.getAttrRepo(customerSpace, version), query);
    }

    public DataPage getData(AttributeRepository attrRepo, Query query) {
        List<Map<String, Object>> results = getDataFlux(attrRepo, query).collectList().block();
        return new DataPage(results);
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
        AtomicLong startTime = new AtomicLong();
        AtomicLong counter = new AtomicLong(0);
        return queryEvaluator.pipe(sqlQuery, query) //
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
