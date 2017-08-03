package com.latticeengines.proxy.exposed.metadata;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("dataCollectionProxy")
public class DataCollectionProxy extends MicroserviceRestApiProxy {

    private static final Logger log = LoggerFactory.getLogger(DataCollectionProxy.class);

    private LoadingCache<String, AttributeRepository> attrRepoCache = null;
    private LoadingCache<String, StatisticsContainer> statsCache = null;

    protected DataCollectionProxy() {
        super("metadata");
    }

    public DataCollection getDefaultDataCollection(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection",
                shortenCustomerSpace(customerSpace));
        return get("get default dataCollection", url, DataCollection.class);
    }

    public AttributeRepository getAttrRepo(String customerSpace) {
        if (attrRepoCache == null) {
            initializeAttrRepoCache();
        }
        return attrRepoCache.get(customerSpace);
    }

    public StatisticsContainer getStats(String customerSpace) {
        if (statsCache == null) {
            initializeStatsCache();
        }
        return statsCache.get(customerSpace);
    }

    public Table getTable(String customerSpace, TableRoleInCollection tableRole) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datacollection/tables?role={tableRole}", //
                shortenCustomerSpace(customerSpace), tableRole);
        return get("getTable", url, Table.class);
    }

    public void resetTable(String customerSpace, TableRoleInCollection tableRole) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datacollection/reset?role={tableRole}", //
                shortenCustomerSpace(customerSpace), tableRole);
        post("resetTable", url, null, Table.class);
    }

    public void upsertTable(String customerSpace, String tableName, TableRoleInCollection role) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datacollection/tables/{tableName}?role={role}",
                shortenCustomerSpace(customerSpace), tableName, role);
        post("upsertTable", url, null, DataCollection.class);
    }

    public void upsertStats(String customerSpace, StatisticsContainer container) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/stats",
                shortenCustomerSpace(customerSpace));
        evictStatsCache(customerSpace);
        evictAttrRepoCache(customerSpace);
        post("upsertStats", url, container, SimpleBooleanResponse.class);
    }

    private void initializeStatsCache() {
        statsCache = Caffeine.newBuilder()
                .maximumSize(100)
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .refreshAfterWrite(1, TimeUnit.MINUTES)
                .build(this::getStatsViaRestCall);
        log.info("Initialized loading cache statsCache.");
    }

    private void initializeAttrRepoCache() {
        attrRepoCache = Caffeine.newBuilder()
                .maximumSize(100)
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .refreshAfterWrite(1, TimeUnit.MINUTES)
                .build(this::getAttrRepoViaRestCall);
        log.info("Initialized loading cache attrRepoCache.");
    }

    private void evictAttrRepoCache(String customerSpace) {
        if (attrRepoCache != null) {
            attrRepoCache.invalidate(customerSpace);
            log.info("Invalidated attr repo cache for customer " + shortenCustomerSpace(customerSpace));
        }
    }

    private void evictStatsCache(String customerSpace) {
        if (statsCache != null) {
            statsCache.invalidate(customerSpace);
            log.info("Invalidated stats cache for customer " + shortenCustomerSpace(customerSpace));
        }
    }

    private AttributeRepository getAttrRepoViaRestCall(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/attrrepo",
                shortenCustomerSpace(customerSpace));
        return get("get default attribute repo", url, AttributeRepository.class);
    }

    private StatisticsContainer getStatsViaRestCall(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/stats",
                shortenCustomerSpace(customerSpace));
        return get("get stats", url, StatisticsContainer.class);
    }

}
