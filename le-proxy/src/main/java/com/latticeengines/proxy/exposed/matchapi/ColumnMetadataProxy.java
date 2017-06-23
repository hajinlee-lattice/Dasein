package com.latticeengines.proxy.exposed.matchapi;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ZK_WATCHER_AM_API_UPDATE;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ZK_WATCHER_AM_RELEASE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.latticeengines.camille.exposed.watchers.NodeWatcher;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.network.exposed.propdata.ColumnMetadataInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

import javax.annotation.PostConstruct;

@Component("columnMetadataProxyMatchapi")
public class ColumnMetadataProxy extends BaseRestApiProxy implements ColumnMetadataInterface {

    private static Log log = LogFactory.getLog(ColumnMetadataProxy.class);
    private static final String DEFAULT = "default";
    private static final String AM_REPO = "AMCollection";

    private Cache<String, List<ColumnMetadata>> enrichmentColumnsCache;
    private Cache<String, DataCloudVersion> latestDataCloudVersionCache;
    private Cache<String, AttributeRepository> amAttrRepoCache;

    @Autowired
    @Qualifier("commonTaskScheduler")
    private ThreadPoolTaskScheduler scheduler;

    public ColumnMetadataProxy() {
        super(PropertyUtils.getProperty("common.matchapi.url"), "/match/metadata");
        NodeWatcher.registerWatcher(ZK_WATCHER_AM_API_UPDATE);
        NodeWatcher.registerWatcher(ZK_WATCHER_AM_RELEASE);
        NodeWatcher.registerListener(ZK_WATCHER_AM_API_UPDATE, () -> {
            log.info("am update zk watch changed, updating caches.");
            try {
                // wait 10 sec for match api itself to refresh.
                Thread.sleep(10000L);
            } catch (InterruptedException e) {
                // ignore
            }
            refreshCache();
        });
    }

    @PostConstruct
    private void postConstruct() {
        scheduler.schedule(this::initializeEnrichmentColumnsCache,
                new Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10)));
    }

    @Override
    public List<ColumnMetadata> columnSelection(Predefined selectName, String dataCloudVersion) {
        if (Predefined.Enrichment.equals(selectName)) {
            try {
                if (StringUtils.isEmpty(dataCloudVersion)) {
                    dataCloudVersion = DEFAULT;
                }
                if (enrichmentColumnsCache == null) {
                    initializeEnrichmentColumnsCache();
                }
                return enrichmentColumnsCache.getIfPresent(dataCloudVersion);
            } catch (Exception e) {
                throw new RuntimeException("Failed to get enrichment column metadata from loading cache.", e);
            }
        } else {
            return requestColumnSelection(selectName, dataCloudVersion);
        }
    }

    @Override
    public DataCloudVersion latestVersion(String compatibleVersion) {
        try {
            if (StringUtils.isEmpty(compatibleVersion)) {
                compatibleVersion = DEFAULT;
            }
            if (latestDataCloudVersionCache == null) {
                initializeLatestDataCloudVersionCache();
            }
            return latestDataCloudVersionCache.getIfPresent(compatibleVersion);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get latest version for dataCloudVersion " //
                    + compatibleVersion + " from loading cache.", e);
        }
    }

    @SuppressWarnings({ "unchecked" })
    private List<ColumnMetadata> requestColumnSelection(Predefined selectName, String dataCloudVersion) {
        String url = constructUrl("/predefined/{selectName}", String.valueOf(selectName.name()));
        if (StringUtils.isNotBlank(dataCloudVersion)) {
            url = constructUrl("/predefined/{selectName}?datacloudversion={dataCloudVersion}",
                    String.valueOf(selectName.name()), dataCloudVersion);
        }
        List<Map<String, Object>> metadataObjs = get("columnSelection", url, List.class);
        List<ColumnMetadata> metadataList = new ArrayList<>();
        if (metadataObjs == null) {
            return metadataList;
        }

        ObjectMapper mapper = new ObjectMapper();
        try {
            for (Map<String, Object> obj : metadataObjs) {
                ColumnMetadata metadata = mapper.treeToValue(mapper.valueToTree(obj), ColumnMetadata.class);
                metadataList.add(metadata);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return metadataList;
    }

    @SuppressWarnings({ "unchecked" })
    public AttributeRepository getAttrRepo() {
        if (amAttrRepoCache == null) {
            initializeAmAttrRepoCache();
        }
        return amAttrRepoCache.getIfPresent(AM_REPO);
    }

    private AttributeRepository getAttrRepoViaREST() {
        String url = constructUrl("/attrrepo");
        return get("get AM attr repo", url, AttributeRepository.class);
    }

    private DataCloudVersion requestLatestVersion(String compatibleVersion) {
        String url;
        if (StringUtils.isNotBlank(compatibleVersion)) {
            url = constructUrl("/versions/latest?compatibleto={compatibleVersion}", compatibleVersion);
        } else {
            url = constructUrl("/versions/latest");
        }
        DataCloudVersion latestVersion = get("latest version", url, DataCloudVersion.class);
        return latestVersion;
    }

    private void loadEnrichmenColumnsCache(String dataCloudVersion) {
        if (DEFAULT.equals(dataCloudVersion)) {
            dataCloudVersion = "";
        }
        List<ColumnMetadata> columns = requestColumnSelection(Predefined.Enrichment, dataCloudVersion);
        if (columns != null && !columns.isEmpty()) {
            enrichmentColumnsCache.put(dataCloudVersion, columns);
            log.info("Loaded " + columns.size() + " columns into LoadingCache.");
        } else {
            log.warn("Got empty list when attempting to refresh enrichmentColumnsCache for " + dataCloudVersion
                    + ". Keep the old value.");
        }
    }

    private void loadLatestDataCloudVersionCache(String compatibleVersion) {
        if (DEFAULT.equals(compatibleVersion)) {
            compatibleVersion = "";
        }
        DataCloudVersion latestVersion = requestLatestVersion(compatibleVersion);
        if (latestVersion != null) {
            latestDataCloudVersionCache.put(compatibleVersion, latestVersion);
            log.info("Loaded latest version for compatibleVersion '" + compatibleVersion + "' into LoadingCache: "
                    + latestVersion.getVersion());
        } else {
            log.warn("Got null when attempting to refresh latestDataCloudVersionCache for " + compatibleVersion
                    + ". Keep the old value.");
        }
    }

    private void loadAmAttrRepoCache() {
        AttributeRepository attrRepo = getAttrRepoViaREST();
        if (attrRepo != null) {
            amAttrRepoCache.put(DEFAULT, getAttrRepoViaREST());
        } else {
            log.warn("Got null when attempting to refresh amAttrRepoCache. Keep the old value.");
        }
    }

    private void initializeEnrichmentColumnsCache() {
        if (enrichmentColumnsCache == null) {
            log.info("Initializing enrichmentColumnsCache.");
            enrichmentColumnsCache = CacheBuilder.newBuilder().maximumSize(20).build();
            loadEnrichmenColumnsCache(DEFAULT);
        }
    }

    private void initializeLatestDataCloudVersionCache() {
        if (latestDataCloudVersionCache == null) {
            log.info("Initializing latestDataCloudVersionCache.");
            latestDataCloudVersionCache = CacheBuilder.newBuilder().maximumSize(20).build();
            loadLatestDataCloudVersionCache(DEFAULT);
        }
    }

    private void initializeAmAttrRepoCache() {
        if (amAttrRepoCache == null) {
            log.info("Initializing amAttrRepoCache.");
            amAttrRepoCache = CacheBuilder.newBuilder().maximumSize(1).build();
            loadAmAttrRepoCache();
        }
    }

    private void refreshCache() {
        log.info("Updating the caches in column metadata proxy due to zk change.");
        if (enrichmentColumnsCache != null) {
            for (String key : enrichmentColumnsCache.asMap().keySet()) {
                loadEnrichmenColumnsCache(key);
            }
        }
        if (latestDataCloudVersionCache != null) {
            for (String key : latestDataCloudVersionCache.asMap().keySet()) {
                loadLatestDataCloudVersionCache(key);
            }
        }
        if (amAttrRepoCache != null) {
            loadAmAttrRepoCache();
        }
    }

}
