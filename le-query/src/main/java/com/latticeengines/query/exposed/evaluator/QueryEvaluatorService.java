package com.latticeengines.query.exposed.evaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.querydsl.sql.SQLQuery;

@Component("queryEvaluatorService")
public class QueryEvaluatorService {

    private static final Logger log = LoggerFactory.getLogger(QueryEvaluatorService.class);

    @Autowired
    private DataCollectionProxy dataCollectionProxy; // attr repo cached in this proxy

    @Autowired
    private QueryEvaluator queryEvaluator;

    public long getCount(String customerSpace, Query query) {
        return getCount(dataCollectionProxy.getAttrRepo(customerSpace), query);
    }

    public long getCount(AttributeRepository attrRepo, Query query) {
        long count = -1;
        swapLatticeAccountSelects(attrRepo, query);
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
        swapLatticeAccountSelects(attrRepo, query);
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

    //TODO: temporary solution until all tenants migrate to denormalized Account table
    private void swapLatticeAccountSelects(AttributeRepository attrRepo, Query query) {
        if (query.getLookups() != null) {
            List<Lookup> lookups = new ArrayList<>();
            query.getLookups().forEach(lookup -> {
                if (lookup instanceof AttributeLookup) {
                    AttributeLookup attributeLookup = (AttributeLookup) lookup;
                    if (BusinessEntity.LatticeAccount.equals(attributeLookup.getEntity())) {
                        AttributeLookup attributeLookup2 = new AttributeLookup(BusinessEntity.Account, attributeLookup.getAttribute());
                        if (attrRepo.getColumnMetadata(attributeLookup2) != null) {
                            lookups.add(attributeLookup2);
                            log.info("Replaced " + attributeLookup + " by " + attributeLookup2);
                        } else {
                            lookups.add(attributeLookup);
                        }
                    } else if (attrRepo.getColumnMetadata(attributeLookup) != null) {
                        lookups.add(attributeLookup);
                    } else {
                        log.warn("Cannot find " + attributeLookup + " in attribute repository, skipping. it");
                    }
                } else {
                    lookups.add(lookup);
                }
            });
            query.setLookups(lookups);
        }
    }

}
