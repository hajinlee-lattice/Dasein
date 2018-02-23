package com.latticeengines.proxy.exposed.matchapi;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.support.CompositeCacheManager;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.cache.exposed.cachemanager.LocalCacheManager;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.network.exposed.propdata.ColumnMetadataInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Component("columnMetadataProxyMatchapi")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class ColumnMetadataProxy extends BaseRestApiProxy implements ColumnMetadataInterface {
    private static final Logger log = LoggerFactory.getLogger(ColumnMetadataProxy.class);

    private static final String STATS_CUBE = "StatsCube";
    private static final String TOPN_TREE = "TopNTree";

    // has to be public because used in cache key
    public static final String KEY_PREFIX = DataCloudConstants.SERVICE_TENANT;

    private LocalCacheManager<String, List<ColumnMetadata>> columnMetadataCache = null;
    private LocalCacheManager<String, DataCloudVersion> latestDataCloudVersionCache;
    private LocalCacheManager<String, Object> amStatsCache;

    private final CacheManager cacheManager;
    private Scheduler parallelFluxThreadPool;

    @Inject
    public ColumnMetadataProxy(CacheManager cacheManager) {
        super(PropertyUtils.getProperty("common.matchapi.url"), "/match/metadata");
        this.cacheManager = cacheManager;
        latestDataCloudVersionCache = new LocalCacheManager<>(CacheName.DataCloudVersionCache, str -> {
            String key = str.split("\\|")[1];
            return requestLatestVersion(key);
        }, 10);

    }

    @SuppressWarnings("unchecked")
    @PostConstruct
    private void postConstruct() {
        if (cacheManager instanceof CompositeCacheManager) {
            log.info("adding local " + CacheName.DataCloudVersionCache + " to composite cache manager");
            ((CompositeCacheManager) cacheManager).setCacheManagers(Collections.singleton(latestDataCloudVersionCache));
        }
    }

    public void scheduleLoadColumnMetadataCache() {
        Mono.delay(Duration.of(10, ChronoUnit.MINUTES)).map(k -> getAllColumns()).subscribe();
    }

    public Set<String> premiumAttributes(String dataCloudVersion) {
        List<ColumnMetadata> allColumns = getAllColumns(dataCloudVersion);
        return allColumns.stream() //
                .filter(cm -> StringUtils.isNotBlank(cm.getDataLicense())) //
                .map(ColumnMetadata::getColumnId) //
                .collect(Collectors.toSet());
    }

    public List<ColumnMetadata> columnSelection(Predefined selectName) {
        return columnSelection(selectName, "");
    }

    @Override
    public List<ColumnMetadata> columnSelection(Predefined selectName, String dataCloudVersion) {
        String msg = "Load mdatadata of predefined selection " + selectName + " data cloud version "
                + dataCloudVersion;
        try (PerformanceTimer timer = new PerformanceTimer(msg)) {
            if (dataCloudVersion.startsWith("1.0")) {
                return requestColumnSelection(selectName, dataCloudVersion);
            } else {
                List<ColumnMetadata> allColumns = getAllColumns(dataCloudVersion);
                return Flux.fromIterable(allColumns) //
                        .parallel().runOn(parallelFluxThreadPool()) //
                        .filter(cm -> cm.isEnabledFor(selectName)) //
                        .sequential().collectList() //
                        .blockOptional(Duration.of(2, ChronoUnit.MINUTES)).orElse(null);
            }
        }
    }

    @Cacheable(cacheNames = CacheName.Constants.DataCloudVersionCacheName, key = "T(java.lang.String).format(\"%s|%s|latest\", T(com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy).KEY_PREFIX, #compatibleVersion)", sync = true)
    public DataCloudVersion latestVersion(String compatibleVersion) {
        return requestLatestVersion(compatibleVersion);
    }

    public List<ColumnMetadata> getAllColumns() {
        return getAllColumns("");
    }

    private synchronized List<ColumnMetadata> getAllColumns(String dataCloudVersion) {
        if (StringUtils.isEmpty(dataCloudVersion)) {
            dataCloudVersion = "";
        }
        initializeColumnMetadataCache();
        return columnMetadataCache.getWatcherCache().get(KEY_PREFIX + "|" + dataCloudVersion);
    }

    private List<ColumnMetadata> requestAllColumns(String dataCloudVersion) {
        String msg = "Load mdatadata of data cloud version " + dataCloudVersion;
        try (PerformanceTimer timer = new PerformanceTimer(msg)) {
            long count = getColumnCount(dataCloudVersion);
            int pageSize = 2000;
            int numPages = (int) Math.ceil(1.0 * count / pageSize);
            Flux<ColumnMetadata> flux = Flux.range(0, numPages) //
                    .parallel().runOn(parallelFluxThreadPool()) //
                    .flatMap(page -> requestMetadataPage(dataCloudVersion, page, pageSize))
                    .sequential();
            List<ColumnMetadata> cms = flux.collectList() //
                    .blockOptional(Duration.of(1, ChronoUnit.MINUTES)) //
                    .orElse(null);
            if (cms != null) {
                log.info("Loaded in total " + cms.size() + " columns from matchapi");
            }
            return cms;
        }
    }

    private Flux<ColumnMetadata> requestMetadataPage(String dataCloudVersion, int page, int size) {
        String url = constructUrl("/?page={page}&size={size}", page, size);
        if (StringUtils.isNotBlank(dataCloudVersion)) {
            url += "&datacloudversion=" + dataCloudVersion;
        }
        @SuppressWarnings("unchecked")
        List<ColumnMetadata> metadataList = getKryo("get metadata page", url, List.class);
        if (CollectionUtils.isEmpty(metadataList)) {
            return Flux.empty();
        } else {
            log.info("Retrieved " + metadataList.size() + " column metadata from matchapi.");
            return Flux.fromIterable(metadataList);
        }
    }

    private Long getColumnCount(String dataCloudVersion) {
        if (StringUtils.isBlank(dataCloudVersion)) {
            dataCloudVersion = latestVersion("").getVersion();
        }
        String url = constructUrl("/count?datacloudversion={dataCloudVersion}", dataCloudVersion);
        Long count = get("get count", url, Long.class);
        if (count == null || count == 0) {
            throw new IllegalStateException("There is no metadata in data cloud version " + dataCloudVersion);
        } else {
            return count;
        }
    }

    public StatsCube getStatsCube() {
        initializeAMStatsCache();
        return (StatsCube) amStatsCache.getWatcherCache().get(KEY_PREFIX + "|" + STATS_CUBE);
    }

    public TopNTree getTopNTree() {
        initializeAMStatsCache();
        return (TopNTree) amStatsCache.getWatcherCache().get(KEY_PREFIX + "|" + TOPN_TREE);
    }

    private Object getStatsObjectViaREST(String key) {
        switch (key) {
        case STATS_CUBE:
            return getKryo("get AM status cube", constructUrl("/statscube"), StatsCube.class);
        case TOPN_TREE:
            return getKryo("get AM top n tree", constructUrl("/topn"), TopNTree.class);
        default:
            throw new IllegalArgumentException("Unknown cache key " + key);
        }
    }

    private DataCloudVersion requestLatestVersion(String compatibleVersion) {
        String url;
        if (StringUtils.isNotBlank(compatibleVersion)) {
            url = constructUrl("/versions/latest?compatibleto={compatibleVersion}", compatibleVersion);
        } else {
            url = constructUrl("/versions/latest");
        }
        return get("latest version", url, DataCloudVersion.class);
    }

    private synchronized void initializeColumnMetadataCache() {
        if (columnMetadataCache == null) {
            columnMetadataCache = new LocalCacheManager<>( //
                    CacheName.DataCloudCMCache, //
                    str -> {
                        String key = str.replace(KEY_PREFIX + "|", "");
                        return requestAllColumns(key);
                    }, //
                    10); //
            log.info("Initialized local cache DataCloudCMCache.");
        }
    }

    private synchronized void initializeAMStatsCache() {
        if (amStatsCache == null) {
            amStatsCache = new LocalCacheManager<>(CacheName.DataCloudStatsCache, str -> {
                String key = str.replace(KEY_PREFIX + "|", "");
                return getStatsObjectViaREST(key);
            }, 10);
            log.info("Initialized local cache DataCloudStatsCache.");
        }
    }

    // only for 1.0.0
    @SuppressWarnings({ "unchecked" })
    private List<ColumnMetadata> requestColumnSelection(Predefined selectName, String dataCloudVersion) {
        String url = constructUrl("/predefined/{selectName}", String.valueOf(selectName.name()));
        if (StringUtils.isNotBlank(dataCloudVersion)) {
            url = constructUrl("/predefined/{selectName}?datacloudversion={dataCloudVersion}",
                    String.valueOf(selectName.name()), dataCloudVersion);
        }
        List<ColumnMetadata> metadataList = getKryo("columnSelection", url, List.class);
        if (CollectionUtils.isEmpty(metadataList)) {
            return Collections.emptyList();
        } else {
            return metadataList;
        }
    }

    private synchronized Scheduler parallelFluxThreadPool() {
        if (parallelFluxThreadPool == null) {
            parallelFluxThreadPool = Schedulers.newParallel("column-metadata");
        }
        return parallelFluxThreadPool;
    }

}
