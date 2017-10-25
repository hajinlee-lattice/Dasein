package com.latticeengines.proxy.exposed.metadata;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("dataCollectionProxy")
@Scope( proxyMode = ScopedProxyMode.TARGET_CLASS )
@CacheConfig(cacheNames = "MetadataCache")
public class DataCollectionProxy extends MicroserviceRestApiProxy {

    private static final Logger log = LoggerFactory.getLogger(DataCollectionProxy.class);

    private LoadingCache<String, AttributeRepository> attrRepoCache = null;

    protected DataCollectionProxy() {
        super("metadata");
    }

    public DataCollection getDefaultDataCollection(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection",
                shortenCustomerSpace(customerSpace));
        return get("get default dataCollection", url, DataCollection.class);
    }

    public void switchVersion(String customerSpace, DataCollection.Version version) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/version/{version}",
                shortenCustomerSpace(customerSpace), version);
        put("get default dataCollection", url, ResponseDocument.class);
    }

    @Cacheable(key = "T(java.lang.String).format(\"%s|attrrepo\", #customerSpace)")
    public AttributeRepository getAttrRepo(String customerSpace) {
        // if (attrRepoCache == null) {
        // initializeAttrRepoCache();
        // }
        // return attrRepoCache.get(customerSpace);
        return getAttrRepoViaRestCall(customerSpace);
    }

    public StatisticsContainer getStats(String customerSpace) {
        return getStats(customerSpace, null);
    }

    public StatisticsContainer getStats(String customerSpace, DataCollection.Version version) {
        String urlPattern = "/customerspaces/{customerSpace}/datacollection/stats";
        List<Object> args = new ArrayList<>();
        args.add(shortenCustomerSpace(customerSpace));
        if (version != null) {
            urlPattern += "?version={version}";
            args.add(version);
        }
        String url = constructUrl(urlPattern, args.toArray(new Object[args.size()]));
        return get("get stats", url, StatisticsContainer.class);
    }

    public Table getTable(String customerSpace, TableRoleInCollection role) {
        return getTable(customerSpace, role, null);
    }

    public Table getTable(String customerSpace, TableRoleInCollection role, DataCollection.Version version) {
        String urlPattern = "/customerspaces/{customerSpace}/datacollection/tables?role={role}";
        List<Object> args = new ArrayList<>();
        args.add(shortenCustomerSpace(customerSpace));
        args.add(role);
        if (version != null) {
            urlPattern += "&version={version}";
            args.add(version);
        }
        String url = constructUrl(urlPattern, args.toArray(new Object[args.size()]));
        return get("getTable", url, Table.class);
    }

    @SuppressWarnings("rawtypes")
    public List<MetadataSegment> getSegments(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/segments",
                shortenCustomerSpace(customerSpace));

        List raw = get("getSegments", url, List.class);
        return JsonUtils.convertList(raw, MetadataSegment.class);
    }

    public void resetTable(String customerSpace, TableRoleInCollection tableRole) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/reset?role={tableRole}", //
                shortenCustomerSpace(customerSpace), tableRole);
        post("resetTable", url, null, Table.class);
    }

    public void upsertTable(String customerSpace, String tableName, TableRoleInCollection role,
            DataCollection.Version version) {
        String urlPattern = "/customerspaces/{customerSpace}/datacollection/tables/{tableName}?role={role}";
        List<Object> args = new ArrayList<>();
        args.add(shortenCustomerSpace(customerSpace));
        args.add(tableName);
        args.add(role);
        if (version != null) {
            urlPattern += "&version={version}";
            args.add(version);
        }
        String url = constructUrl(urlPattern, args.toArray(new Object[args.size()]));
        post("upsertTable", url, null, DataCollection.class);
    }

    @CacheEvict(key = "T(java.lang.String).format(\"%s|attrrepo\", #customerSpace)")
    public void upsertStats(String customerSpace, StatisticsContainer container) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/stats",
                shortenCustomerSpace(customerSpace));
        // evictAttrRepoCache(customerSpace);
        post("upsertStats", url, container, SimpleBooleanResponse.class);
    }

    @Deprecated
    private void initializeAttrRepoCache() {
        attrRepoCache = Caffeine.newBuilder().maximumSize(100).expireAfterWrite(5, TimeUnit.MINUTES)
                .refreshAfterWrite(1, TimeUnit.MINUTES).build(this::getAttrRepoViaRestCall);
        log.info("Initialized loading cache attrRepoCache.");
    }

    @Deprecated
    private void evictAttrRepoCache(String customerSpace) {
        if (attrRepoCache != null) {
            attrRepoCache.invalidate(customerSpace);
            log.info("Invalidated attr repo cache for customer " + shortenCustomerSpace(customerSpace));
        }
    }

    private AttributeRepository getAttrRepoViaRestCall(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/attrrepo",
                shortenCustomerSpace(customerSpace));
        return get("get default attribute repo", url, AttributeRepository.class);
    }

    public DataCollection.Version getActiveVersion(String customerSpace) {
        return getDefaultDataCollection(customerSpace).getVersion();
    }

    public DataCollection.Version getInactiveVersion(String customerSpace) {
        DataCollection.Version activeVersion = getActiveVersion(customerSpace);
        return activeVersion.complement();
    }

}
