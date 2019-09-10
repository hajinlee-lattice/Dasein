package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cache.exposed.cachemanager.LocalCacheManager;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.cdl.CDLDataSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatusHistory;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;

@Component("dataCollectionProxy")
public class DataCollectionProxy extends MicroserviceRestApiProxy {

    private static final Logger log = LoggerFactory.getLogger(DataCollectionProxy.class);

    private LocalCacheManager<String, AttributeRepository> attrRepoCache = null;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private DataUnitProxy dataUnitProxy;

    public DataCollectionProxy() {
        super("cdl");
    }

    public DataCollection getDefaultDataCollection(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection",
                shortenCustomerSpace(customerSpace));
        return get("get default dataCollection", url, DataCollection.class);
    }

    public void switchVersion(String customerSpace, DataCollection.Version version) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/version/{version}",
                shortenCustomerSpace(customerSpace), version);
        put("get default dataCollection", url);
    }

    @Deprecated
    public void updateDataCloudBuildNumber(String customerSpace, String dataCloudBuildNumber) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datacollection/datacloudbuildnumber/{dataCloudBuildNumber}",
                shortenCustomerSpace(customerSpace), dataCloudBuildNumber);
        put("update dataCollection datacloudbuildnumber", url);
    }

    public DataCollectionStatus getOrCreateDataCollectionStatus(String customerSpace,
            DataCollection.Version version) {
        String urlPattern = "/customerspaces/{customerSpace}/datacollection/status";
        List<Object> args = new ArrayList<>();
        args.add(shortenCustomerSpace(customerSpace));
        if (version != null) {
            urlPattern += "?version={version}";
            args.add(version);
        }
        String url = constructUrl(urlPattern, args.toArray(new Object[0]));
        return get("get dataCollection status", url, DataCollectionStatus.class);
    }

    public void saveOrUpdateDataCollectionStatus(String customerSpace, DataCollectionStatus detail,
            DataCollection.Version version) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/version/{version}/status",
                shortenCustomerSpace(customerSpace), version);
        post("save Or Update Status", url, detail, Void.class);
    }

    public void saveDataCollectionStatusHistory(String customerSpace, DataCollectionStatus detail) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/statushistory",
                shortenCustomerSpace(customerSpace));
        post("save status history", url, detail, Void.class);
    }

    public List<DataCollectionStatusHistory> getDataCollectionStatusHistory(String customerSpace) {
        String urlPattern = "/customerspaces/{customerSpace}/datacollection/statushistory";
        List<Object> args = new ArrayList<>();
        args.add(shortenCustomerSpace(customerSpace));
        String url = constructUrl(urlPattern, args.toArray(new Object[0]));
        return getList("get dataCollection status history", url, DataCollectionStatusHistory.class);
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
        String url = constructUrl(urlPattern, args.toArray(new Object[0]));
        return getKryo("get stats", url, StatisticsContainer.class);
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
        String url = constructUrl(urlPattern, args.toArray(new Object[0]));
        return get("getTable", url, Table.class);
    }

    public List<Table> getTables(String customerSpace, TableRoleInCollection role, DataCollection.Version version) {
        String urlPattern = "/customerspaces/{customerSpace}/datacollection/alltables?role={role}";
        List<Object> args = new ArrayList<>();
        args.add(shortenCustomerSpace(customerSpace));
        args.add(role);
        if (version != null) {
            urlPattern += "&version={version}";
            args.add(version);
        }
        String url = constructUrl(urlPattern, args.toArray(new Object[0]));
        List<?> list = get("getTables", url, List.class);
        return JsonUtils.convertList(list, Table.class);
    }

    public String getTableName(String customerSpace, TableRoleInCollection role) {
        return getTableName(customerSpace, role, null);
    }

    public String getTableName(String customerSpace, TableRoleInCollection role, DataCollection.Version version) {
        List<String> tableNames = getTableNames(customerSpace, role, version);
        if (CollectionUtils.isNotEmpty(tableNames)) {
            return tableNames.get(0);
        } else {
            return null;
        }
    }

    public List<String> getTableNames(String customerSpace, DataCollection.Version version) {
        return getTableNames(customerSpace, null, version);
    }

    public List<String> getTableNames(String customerSpace, TableRoleInCollection role,
            DataCollection.Version version) {
        String urlPattern = "/customerspaces/{customerSpace}/datacollection/tablenames";
        List<Object> args = new ArrayList<>();
        List<String> conditions = new ArrayList<>();
        args.add(shortenCustomerSpace(customerSpace));
        if (role != null) {
            conditions.add("role={role}");
            args.add(role);
        }
        if (version != null) {
            conditions.add("version={version}");
            args.add(version);
        }
        if (CollectionUtils.isNotEmpty(conditions)) {
            urlPattern += "?" + StringUtils.join(conditions, "&");
        }
        String url = constructUrl(urlPattern, args.toArray(new Object[0]));
        List<?> list = get("getTableNames", url, List.class);
        return JsonUtils.convertList(list, String.class);
    }

    public Map<String, String> getTableNamesWithSignatures(String customerSpace, TableRoleInCollection role,
            DataCollection.Version version, List<String> signatures) {
        String urlPattern = "/customerspaces/{customerSpace}/datacollection/tablenames/signatures";
        List<Object> args = new ArrayList<>();
        List<String> conditions = new ArrayList<>();
        args.add(shortenCustomerSpace(customerSpace));
        if (role != null) {
            conditions.add("role={role}");
            args.add(role);
        }
        if (version != null) {
            conditions.add("version={version}");
            args.add(version);
        }
        if (signatures != null) {
            conditions.add("signatures={signatures}");
            args.add(String.join(",", signatures));
        }
        if (CollectionUtils.isNotEmpty(conditions)) {
            urlPattern += "?" + StringUtils.join(conditions, "&");
        }
        String url = constructUrl(urlPattern, args.toArray(new Object[0]));
        Map<?, ?> map = get("getTableNamesWithSignatures", url, Map.class);
        return JsonUtils.convertMap(map, String.class, String.class);
    }

    @SuppressWarnings("rawtypes")
    public List<MetadataSegment> getSegments(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/segments",
                shortenCustomerSpace(customerSpace));

        List raw = get("getSegments", url, List.class);
        return JsonUtils.convertList(raw, MetadataSegment.class);
    }

    public void resetTable(String customerSpace, TableRoleInCollection tableRole) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/resettables?role={tableRole}", //
                shortenCustomerSpace(customerSpace), tableRole);
        post("resetTable", url, null, Table.class);
    }

    public void upsertTable(String customerSpace, String tableName, TableRoleInCollection role,
            DataCollection.Version version) {
        String urlPattern = "/customerspaces/{customerSpace}/datacollection/tables/{targetTableName}?role={role}";
        List<Object> args = new ArrayList<>();
        args.add(shortenCustomerSpace(customerSpace));
        args.add(tableName);
        args.add(role);
        if (version != null) {
            urlPattern += "&version={version}";
            args.add(version);
        }
        String url = constructUrl(urlPattern, args.toArray(new Object[0]));
        postKryo("upsertTable", url, null, null);
    }

    public void upsertTables(String customerSpace, List<String> tableNames, TableRoleInCollection role,
            DataCollection.Version version) {
        String urlPattern = "/customerspaces/{customerSpace}/datacollection/tables/multi/{targetTableName}?role={role}";
        List<Object> args = new ArrayList<>();
        args.add(shortenCustomerSpace(customerSpace));
        args.add(String.join(",", tableNames));
        args.add(role);
        if (version != null) {
            urlPattern += "&version={version}";
            args.add(version);
        }
        String url = constructUrl(urlPattern, args.toArray(new Object[0]));
        postKryo("upsertTables", url, null, null);
    }

    public void upsertTablesWithSignatures(String customerSpace, Map<String, String> signatureTableNames,
            TableRoleInCollection role, DataCollection.Version version) {
        String urlPattern = "/customerspaces/{customerSpace}/datacollection/tables/signatures?role={role}";
        List<Object> args = new ArrayList<>();
        args.add(shortenCustomerSpace(customerSpace));
        args.add(role);
        if (version != null) {
            urlPattern += "&version={version}";
            args.add(version);
        }
        String url = constructUrl(urlPattern, args.toArray(new Object[0]));
        postKryo("upsertTablesWithSignatures", url, signatureTableNames, null);
    }

    public void unlinkTable(String customerSpace, String tableName, TableRoleInCollection role,
            DataCollection.Version version) {
        String urlPattern = "/customerspaces/{customerSpace}/datacollection/tables/{targetTableName}?role={role}&version={version}";
        List<Object> args = new ArrayList<>();
        args.add(shortenCustomerSpace(customerSpace));
        args.add(tableName);
        args.add(role);
        args.add(version);
        String url = constructUrl(urlPattern, args.toArray(new Object[args.size()]));
        delete("unlinkTable", url);
    }

    public void unlinkTables(String customerSpace, TableRoleInCollection role, DataCollection.Version version) {
        String urlPattern = "/customerspaces/{customerSpace}/datacollection/tables?role={role}&version={version}";
        List<Object> args = new ArrayList<>();
        args.add(shortenCustomerSpace(customerSpace));
        args.add(role);
        args.add(version);
        String url = constructUrl(urlPattern, args.toArray(new Object[args.size()]));
        delete("unlinkTables", url);
    }

    public void unlinkTables(String customerSpace, DataCollection.Version version) {
        String urlPattern = "/customerspaces/{customerSpace}/datacollection/tables?version={version}";
        List<Object> args = new ArrayList<>();
        args.add(shortenCustomerSpace(customerSpace));
        args.add(version);
        String url = constructUrl(urlPattern, args.toArray(new Object[args.size()]));
        delete("unlinkTables", url);
    }

    public void upsertStats(String customerSpace, StatisticsContainer container) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/stats",
                shortenCustomerSpace(customerSpace));
        evictAttrRepoCache(customerSpace, null);
        postKryo("upsertStats", url, container);
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
            Thread.sleep(5000);
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
            String[] tokens = key.split("\\.");
            if (tokens.length != 2) {
                throw new RuntimeException(
                        "Cache key " + key + " has \".\" but cannot be decomposed into exactly 2 tokens.");
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
            method = "get default attribute repo for customer " + customerSpace  + " at version " + version;
        }
        return getKryo(method, url, AttributeRepository.class);
    }

    public DataCollection.Version getActiveVersion(String customerSpace) {
        return getDefaultDataCollection(customerSpace).getVersion();
    }

    public DataCollection.Version getInactiveVersion(String customerSpace) {
        DataCollection.Version activeVersion = getActiveVersion(customerSpace);
        return activeVersion.complement();
    }

    //FIXME: to be implemented
    public DataCloudVersion getDataCloudVersion(String customerSpace, DataCollection.Version version) {
        return columnMetadataProxy.latestVersion();
    }

    public DynamoDataUnit getAccountDynamo(String customerSpace, DataCollection.Version version) {
        DynamoDataUnit dynamoDataUnit = null;
        String tableName = getTableName(customerSpace, TableRoleInCollection.ConsolidatedAccount, version);
        if (StringUtils.isNotBlank(tableName)) {
            DataUnit dataUnit = dataUnitProxy.getByNameAndType(customerSpace, tableName, DataUnit.StorageType.Dynamo);
            if (dataUnit != null) {
                dynamoDataUnit = (DynamoDataUnit) dataUnit;
            }
        }
        return dynamoDataUnit;
    }

    public CDLDataSpace getCDLDataSpace(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/dataspace",
                shortenCustomerSpace(customerSpace));
        return get("createCDLDataSpace", url, CDLDataSpace.class);
    }

    public List<DynamoDataUnit> getDynamoDataUnits(String customerSpace, DataCollection.Version version,
            List<TableRoleInCollection> tableRoles) {
        List<DynamoDataUnit> dynamoDataUnits = new ArrayList<>();
        for (TableRoleInCollection tableRole : tableRoles) {
            String tableName = getTableName(customerSpace, tableRole, version);
            if (StringUtils.isNotBlank(tableName)) {
                DataUnit dataUnit = dataUnitProxy.getByNameAndType(customerSpace, tableName,
                        DataUnit.StorageType.Dynamo);
                if (dataUnit != null) {
                    dynamoDataUnits.add((DynamoDataUnit) dataUnit);
                }
            }
        }
        return dynamoDataUnits;
    }

    public List<DataCollectionArtifact> getDataCollectionArtifacts(String customerSpace,
            DataCollectionArtifact.Status status, DataCollection.Version version) {
        String url = "/customerspaces/{customerSpace}/datacollection/artifact";
        List<String> params = new ArrayList<>();
        if (status != null) {
            params.add("status=" + status);
        }

        if (version != null) {
            params.add("version=" + version);
        }

        if (CollectionUtils.isNotEmpty(params)) {
            url += "?" + StringUtils.join(params, "&");
        }

        url = constructUrl(url, shortenCustomerSpace(customerSpace));
        List<?> result = get("getDataCollectionArtifacts", url, List.class);
        return JsonUtils.convertList(result, DataCollectionArtifact.class);
    }

    public DataCollectionArtifact getDataCollectionArtifact(String customerSpace, String name,
            DataCollection.Version version) {
        String url = "/customerspaces/{customerSpace}/datacollection/artifact/{name}";
        if (version != null) {
            url += "?version=" + version;
        }

        url = constructUrl(url, shortenCustomerSpace(customerSpace), name);
        return get("getDataCollectionArtifact", url, DataCollectionArtifact.class);
    }

    public DataCollectionArtifact updateDataCollectionArtifact(String customerSpace, DataCollectionArtifact artifact) {
        String url = "/customerspaces/{customerSpace}/datacollection/artifact";
        artifact = put("updateDataCollectionArtifact", constructUrl(url, shortenCustomerSpace(customerSpace)),
                artifact, DataCollectionArtifact.class);
        return artifact;
    }

    public DataCollectionArtifact createDataCollectionArtifact(String customerSpace, DataCollection.Version version,
                                                               DataCollectionArtifact artifact) {
        String requestUrl = constructUrl(
                "/customerspaces/{customerSpace}/datacollection/artifact/version/{version}",
                shortenCustomerSpace(customerSpace), version);
        return post("createDataCollectionArtifact", requestUrl, artifact, DataCollectionArtifact.class);
    }

    public byte[] downloadDataCollectionArtifact(String customerSpace, String exportId) {
        String requestUrl = constructUrl(
                "/customerspaces/{customerSpace}/datacollection/artifact/{exportId}/download",
                shortenCustomerSpace(customerSpace), exportId);
        return getGenericBinary("downloaDataCollectionArtifact", requestUrl, byte[].class);
    }
}
