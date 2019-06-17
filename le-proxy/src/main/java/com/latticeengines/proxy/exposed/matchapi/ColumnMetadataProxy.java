package com.latticeengines.proxy.exposed.matchapi;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import com.latticeengines.cache.exposed.cachemanager.LocalCacheManager;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.network.exposed.propdata.ColumnMetadataInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;
import com.latticeengines.proxy.framework.ProxyRetryTemplate;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Component("columnMetadataProxyMatchapi")
public class ColumnMetadataProxy extends BaseRestApiProxy implements ColumnMetadataInterface {
    private static final Logger log = LoggerFactory.getLogger(ColumnMetadataProxy.class);

    private static final String STATS_CUBE = "StatsCube";
    private static final String TOPN_TREE = "TopNTree";
    private static final String TOPN_TREE_EXCLUDE_INTERNAL = "TopNTreeExcludeInternal";
    private static final String DEFAULT = "default";

    private static final String KEY_PREFIX = DataCloudConstants.SERVICE_TENANT;

    private LocalCacheManager<String, List<ColumnMetadata>> columnMetadataCache = null;
    private LocalCacheManager<String, DataCloudVersion> latestDataCloudVersionCache;
    private LocalCacheManager<String, Object> amStatsCache;

    private Scheduler parallelFluxThreadPool;
    private ExecutorService parallelFetchers;

    public ColumnMetadataProxy() {
        super(PropertyUtils.getProperty("common.matchapi.url"), "/match/metadata");
    }

    public void scheduleLoadColumnMetadataCache() {
        Mono.delay(Duration.of(10, ChronoUnit.MINUTES)).map(k -> getAllColumns()).subscribe();
    }

    public Set<String> premiumAttributes(String dataCloudVersion) {
        List<ColumnMetadata> allColumns = getAllColumns(dataCloudVersion);
        return allColumns.stream() //
                .filter(cm -> Boolean.TRUE.equals(cm.isPremium())) //
                .map(ColumnMetadata::getAttrName) //
                .collect(Collectors.toSet());
    }

    public List<ColumnMetadata> columnSelection(Predefined selectName) {
        return columnSelection(selectName, "");
    }

    @Override
    public List<ColumnMetadata> columnSelection(Predefined selectName, String dataCloudVersion) {
        String msg = "Load mdatadata of predefined selection " + selectName + " data cloud version " + dataCloudVersion;
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

    public String latestBuildNumber() {
        DataCloudVersion latest = requestLatestVersion("");
        return latest.getDataCloudBuildNumber();
    }

    public DataCloudVersion latestVersion() {
        return latestVersion("");
    }

    @Override
    public DataCloudVersion latestVersion(String compatibleVersion) {
        if (StringUtils.isBlank(compatibleVersion)) {
            compatibleVersion = DEFAULT;
        }
        initializeLatestVersionCache();
        DataCloudVersion version = latestDataCloudVersionCache.getWatcherCache()
                .get(KEY_PREFIX + "|" + compatibleVersion);
        if (version == null) {
            throw new NullPointerException("Cannot find latest version compatible with " + compatibleVersion);
        }
        return version;
    }

    public List<ColumnMetadata> getAllColumns() {
        return getAllColumns("");
    }

    public List<ColumnMetadata> getAllColumns(String dataCloudVersion) {
        if (StringUtils.isEmpty(dataCloudVersion)) {
            dataCloudVersion = "";
        }
        initializeColumnMetadataCache();
        List<ColumnMetadata> cms = columnMetadataCache.getWatcherCache().get(KEY_PREFIX + "|" + dataCloudVersion);
        if (CollectionUtils.isEmpty(cms)) {
            throw new RuntimeException(
                    "Cannot find column metadata from cache for DataCloudVersion " + dataCloudVersion);
        }
        return cms;
    }

    private List<ColumnMetadata> requestAllColumnsWithRetry(String dataCloudVersion) {
        ProxyRetryTemplate retry = getRetryTemplate("get AM metadata", HttpMethod.GET, "metadata api", false, null);
        return retry.execute(context -> {
            String msg = "(Attempt=" + (context.getRetryCount() + 1) + ") Load metadata of data cloud version "
                    + dataCloudVersion;
            return requestAllColumns(dataCloudVersion, msg);
        });
    }

    private List<ColumnMetadata> requestAllColumns(String dataCloudVersion, String logMsg) {
        try (PerformanceTimer timer = new PerformanceTimer(logMsg)) {
            long count = getColumnCount(dataCloudVersion);
            int pageSize = 5000;
            int numPages = (int) Math.ceil(1.0 * count / pageSize);
            List<Callable<List<ColumnMetadata>>> callables = new ArrayList<>();
            for (int i = 0; i< numPages; i++) {
                final int page = i;
                Callable<List<ColumnMetadata>> callable = () -> requestMetadataPage(dataCloudVersion, page, pageSize);
                callables.add(callable);
            }
            List<List<ColumnMetadata>> cmLists = ThreadPoolUtils.runCallablesInParallel(parallelFetchers(), //
                    callables, 10, 1);
            List<ColumnMetadata> cms = cmLists.stream().flatMap(Collection::stream).collect(Collectors.toList());
            log.info("Loaded in total " + CollectionUtils.size(cms) + " columns from matchapi");
            return cms;
        }
    }

    private List<ColumnMetadata> requestMetadataPage(String dataCloudVersion, int page, int size) {
        if (StringUtils.isBlank(dataCloudVersion)) {
            dataCloudVersion = latestVersion("").getVersion();
        }
        String url = constructUrl("/?page={page}&size={size}&datacloudversion={dataCloudVersion}", page, size,
                dataCloudVersion);
        return getList("get metadata page", url, ColumnMetadata.class);
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

    /**
     * Retrieve {@link TopNTree} and filter out all the attributes that can be
     * internal enriched if the flag is set
     *
     * @param excludeInternalEnrichment
     *            flag to filter out internal attributes
     * @return
     */
    public TopNTree getTopNTree(boolean excludeInternalEnrichment) {
        initializeAMStatsCache();
        if (excludeInternalEnrichment) {
            return (TopNTree) amStatsCache.getWatcherCache().get(KEY_PREFIX + "|" + TOPN_TREE_EXCLUDE_INTERNAL);
        } else {
            return getTopNTree();
        }
    }

    private Object getStatsObjectViaREST(String key) {
        switch (key) {
        case STATS_CUBE:
            return getKryo("get AM status cube", constructUrl("/statscube"), StatsCube.class);
        case TOPN_TREE:
            return getKryo("get AM top n tree", constructUrl("/topn"), TopNTree.class);
        case TOPN_TREE_EXCLUDE_INTERNAL:
            return filterEnrichment(true);
        default:
            throw new IllegalArgumentException("Unknown cache key " + key);
        }
    }

    private TopNTree filterEnrichment(boolean excludeInternal) {
        TopNTree topNTree = getKryo("get AM top n tree", constructUrl("/topn"), TopNTree.class);
        List<ColumnMetadata> cms = columnSelection(ColumnSelection.Predefined.Enrichment);
        Boolean excludeInternalEnrichment = excludeInternal;
        Set<String> internalAttrs = cms.stream()
                .filter(cm -> excludeInternalEnrichment.equals(cm.getCanInternalEnrich()))
                .map(ColumnMetadata::getAttrName).collect(Collectors.toSet());
        topNTree.getCategories().forEach((cat, catTree) -> catTree.getSubcategories()
                .forEach((subCat, attrs) -> attrs.removeIf(attr -> internalAttrs.contains(attr.getAttribute()))));
        return topNTree;
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

    private void initializeLatestVersionCache() {
        if (latestDataCloudVersionCache == null) {
            synchronized (this) {
                if (latestDataCloudVersionCache == null) {
                    latestDataCloudVersionCache = new LocalCacheManager<>(CacheName.DataCloudVersionCache, str -> {
                        String key = str.split("\\|")[1];
                        if (DEFAULT.equals(key)) {
                            key = "";
                        }
                        return requestLatestVersion(key);
                    }, 10, (int) (Math.random() * 30));
                    log.info("Initialized local cache DataCloudVersionCache.");
                }
            }
        }
    }

    private void initializeColumnMetadataCache() {
        if (columnMetadataCache == null) {
            synchronized (this) {
                if (columnMetadataCache == null) {
                    columnMetadataCache = new LocalCacheManager<>( //
                            CacheName.DataCloudCMCache, //
                            str -> {
                                String key = str.replace(KEY_PREFIX + "|", "");
                                return requestAllColumnsWithRetry(key);
                            }, //
                            10, (int) (Math.random() * 30)); //
                    log.info("Initialized local cache DataCloudCMCache.");
                }
            }
        }
    }

    private void initializeAMStatsCache() {
        if (amStatsCache == null) {
            synchronized (this) {
                if (amStatsCache == null) {
                    amStatsCache = new LocalCacheManager<>(CacheName.DataCloudStatsCache, str -> {
                        String key = str.replace(KEY_PREFIX + "|", "");
                        return getStatsObjectViaREST(key);
                    }, 10, (int) (Math.random() * 30));
                    log.info("Initialized local cache DataCloudStatsCache.");
                }
            }
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

    private synchronized ExecutorService parallelFetchers() {
        if (parallelFetchers == null) {
            parallelFetchers = ThreadPoolUtils.getFixedSizeThreadPool("column-metadata", 4);
        }
        return parallelFetchers;
    }

}
