package com.latticeengines.proxy.exposed.metadata;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cache.exposed.cachemanager.LocalCacheManager;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("dataCollectionProxy")
public class DataCollectionProxy extends MicroserviceRestApiProxy {

    private static final Logger log = LoggerFactory.getLogger(DataCollectionProxy.class);

    private LocalCacheManager<String, AttributeRepository> attrRepoCache = null;

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

    public AttributeRepository getAttrRepo(String customerSpace) {
        return getAttrRepo(customerSpace, null);
    }

    public AttributeRepository getAttrRepo(String customerSpace, DataCollection.Version version) {
        initializeAttrRepoCache();
        String key = constructCacheKey(customerSpace, version);
        return attrRepoCache.getWatcherCache().get(key);
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

    public String getTableName(String customerSpace, TableRoleInCollection role) {
        return getTableName(customerSpace, role, null);
    }

    public String getTableName(String customerSpace, TableRoleInCollection role, DataCollection.Version version) {
        String urlPattern = "/customerspaces/{customerSpace}/datacollection/tablenames?role={role}";
        List<Object> args = new ArrayList<>();
        args.add(shortenCustomerSpace(customerSpace));
        args.add(role);
        if (version != null) {
            urlPattern += "&version={version}";
            args.add(version);
        }
        String url = constructUrl(urlPattern, args.toArray(new Object[args.size()]));
        return get("getTableName", url, String.class);
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

    public void unlinkTable(String customerSpace, String tableName, TableRoleInCollection role,
            DataCollection.Version version) {
        String urlPattern = "/customerspaces/{customerSpace}/datacollection/tables/{tableName}?role={role}&version={version}";
        List<Object> args = new ArrayList<>();
        args.add(shortenCustomerSpace(customerSpace));
        args.add(tableName);
        args.add(role);
        args.add(version);
        String url = constructUrl(urlPattern, args.toArray(new Object[args.size()]));
        delete("unlinkTable", url);
    }

    public void upsertStats(String customerSpace, StatisticsContainer container) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/stats",
                shortenCustomerSpace(customerSpace));
        evictAttrRepoCache(customerSpace, null);
        post("upsertStats", url, container, SimpleBooleanResponse.class);
    }

    public void removeStats(String customerSpace, DataCollection.Version version) {
        if (version == null) {
            throw new IllegalArgumentException("Must specify a version when removing stats.");
        }
        String urlPattern = "/customerspaces/{customerSpace}/datacollection/stats?version={version}";
        String url = constructUrl(urlPattern, shortenCustomerSpace(customerSpace), version);
        delete("remove stats", url);
    }

    private synchronized void initializeAttrRepoCache() {
        if (attrRepoCache == null) {
            attrRepoCache = new LocalCacheManager<>( //
                    CacheName.AttrRepoCache, //
                    this::getAttrRepoViaRestCall, //
                    100); //
            log.info("Initialized loading cache attrRepoCache.");
        }
    }

    public void evictAttrRepoCache(String customerSpace, DataCollection.Version version) {
        initializeAttrRepoCache();
        String key = constructCacheKey(customerSpace, version);
        log.info("Evicting attr repo cache for key " + key);
        attrRepoCache.getCache(CacheName.AttrRepoCache.name()).evict(key);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            log.warn("Thread sleep interrupted.", e);
        }
    }

    private String constructCacheKey(String customerSpace, DataCollection.Version version) {
        String key = shortenCustomerSpace(customerSpace);
        if (version != null) {
            key += "." + version.name();
        }
        return key;
    }

    private Pair<String, DataCollection.Version> parseCacheKey(String key) {
        if (key.contains(".")) {
            String[] tokens =  key.split("\\.");
            if (tokens.length != 2) {
                throw new RuntimeException("Cache key " + key + " has \".\" but cannot be decomposed into exactly 2 tokens.");
            }
            DataCollection.Version version = DataCollection.Version.valueOf(tokens[1]);
            return Pair.of(tokens[0], version);
        } else {
            return Pair.of(key, null);
        }
    }

    private AttributeRepository getAttrRepoViaRestCall(String key) {
        Pair<String, DataCollection.Version> pair = parseCacheKey(key);
        String customerSpace = pair.getLeft();
        DataCollection.Version version = pair.getRight();
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/attrrepo",
                shortenCustomerSpace(customerSpace));
        String method = "get default attribute repo";
        if (version != null) {
            url = constructUrl("/customerspaces/{customerSpace}/datacollection/attrrepo?version={version}",
                    shortenCustomerSpace(customerSpace), version);
            method = "get default attribute repo at version " + version;
        }
        return get(method, url, AttributeRepository.class);
    }

    public DataCollection.Version getActiveVersion(String customerSpace) {
        return getDefaultDataCollection(customerSpace).getVersion();
    }

    public DataCollection.Version getInactiveVersion(String customerSpace) {
        DataCollection.Version activeVersion = getActiveVersion(customerSpace);
        return activeVersion.complement();
    }

}
