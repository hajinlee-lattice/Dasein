package com.latticeengines.proxy.exposed.matchapi;

import static com.latticeengines.domain.exposed.camille.watchers.CamilleWatcher.AMApiUpdate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.exposed.watchers.WatcherCache;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.network.exposed.propdata.ColumnMetadataInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("columnMetadataProxyMatchapi")
public class ColumnMetadataProxy extends BaseRestApiProxy implements ColumnMetadataInterface {

    private static final String STATS_CUBE = "StatsCube";
    private static final String TOPN_TREE = "TopNTree";

    private WatcherCache<String, List<ColumnMetadata>> enrichmentColumnsCache;
    private WatcherCache<String, List<ColumnMetadata>> segmentColumnsCache;
    private WatcherCache<String, DataCloudVersion> latestDataCloudVersionCache;
    private WatcherCache<String, Object> amStatsCache;
    private WatcherCache<String, Set<String>> premiumColumnsCache;


    private Map<Predefined, WatcherCache<String, List<ColumnMetadata>>> columnCacheMap = new HashMap<>();

    private boolean scheduled = false;

    public ColumnMetadataProxy() {
        super(PropertyUtils.getProperty("common.matchapi.url"), "/match/metadata");
    }

    @SuppressWarnings("unchecked")
    @PostConstruct
    private void postConstruct() {
        latestDataCloudVersionCache = WatcherCache.builder() //
                .name("LatestDataCloudVersionCache") //
                .watch(AMApiUpdate) //
                .maximum(20) //
                .load(compatibleVersion -> requestLatestVersion((String) compatibleVersion)) //
                .initKeys(new String[] { "" }) //
                .build();
        enrichmentColumnsCache = WatcherCache.builder() //
                .name("EnrichmentColumnsCache") //
                .watch(AMApiUpdate) //
                .maximum(20) //
                .load(dataCloudVersion -> requestColumnSelection(Predefined.Enrichment, (String) dataCloudVersion)) //
                .initKeys(() -> new String[] { requestLatestVersion("").getVersion() }) //
                .build();
        segmentColumnsCache = WatcherCache.builder() //
                .name("SegmentColumnsCache") //
                .watch(AMApiUpdate) //
                .maximum(20) //
                .load(dataCloudVersion -> requestColumnSelection(Predefined.Segment, (String) dataCloudVersion)) //
                .initKeys(() -> new String[] { requestLatestVersion("").getVersion() }) //
                .build();
        columnCacheMap.put(Predefined.Enrichment, enrichmentColumnsCache);
        columnCacheMap.put(Predefined.Segment, segmentColumnsCache);
        amStatsCache = WatcherCache.builder() //
                .name("AMStatsCubeCache") //
                .watch(AMApiUpdate) //
                .maximum(5) //
                .load(key -> getStatsObjectViaREST((String) key)) //
                .initKeys(new String[] { STATS_CUBE, TOPN_TREE }) //
                .build();
        premiumColumnsCache = WatcherCache.builder() //
                .name("PremiumColumnsCache") //
                .watch(AMApiUpdate) //
                .maximum(20) //
                .load(dataCloudVersion -> requestPremiumColumns((String) dataCloudVersion)) //
                .initKeys(() -> new String[] { requestLatestVersion("").getVersion() }) //
                .build();
    }

    public void scheduleDelayedInitOfEnrichmentColCache() {
        synchronized (this) {
            if (!scheduled) {
                enrichmentColumnsCache.scheduleInit(10, TimeUnit.MINUTES);
                segmentColumnsCache.scheduleInit(15, TimeUnit.MINUTES);
                premiumColumnsCache.scheduleInit(12, TimeUnit.MINUTES);
                scheduled = true;
            }
        }
    }

    @Override
    public Set<String> premiumAttributes(String dataCloudVersion) {
        try {
            return (Set<String>) premiumColumnsCache.get(dataCloudVersion);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get premium columns from watcher cache");
        }
    }

    @Override
    public List<ColumnMetadata> columnSelection(Predefined selectName, String dataCloudVersion) {
        if (columnCacheMap.containsKey(selectName)) {
            try {
                if (StringUtils.isEmpty(dataCloudVersion)) {
                    dataCloudVersion = "";
                }
                return columnCacheMap.get(selectName).get(dataCloudVersion);
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
                compatibleVersion = "";
            }
            return latestDataCloudVersionCache.get(compatibleVersion);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get latest version for dataCloudVersion " //
                    + compatibleVersion + " from loading cache.", e);
        }
    }

    private Set<String> requestPremiumColumns(String dataCloudVersion) {
        Set<String> premiumAttrs = new HashSet<>();
        List<ColumnMetadata> columnSelection = requestColumnSelection(Predefined.Enrichment, dataCloudVersion);
        columnSelection.forEach(column -> {
            if (Boolean.TRUE.equals(column.isPremium())) {
                premiumAttrs.add(column.getColumnId());
            }
        });
        return premiumAttrs;
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

    public StatsCube getStatsCube() {
        return (StatsCube) amStatsCache.get(STATS_CUBE);
    }

    public TopNTree getTopNTree() {
        return (TopNTree) amStatsCache.get(TOPN_TREE);
    }

    private Object getStatsObjectViaREST(String key) {
        switch (key) {
            case STATS_CUBE:
                return get("get AM status cube", constructUrl("/statscube"), StatsCube.class);
            case TOPN_TREE:
                return get("get AM top n tree", constructUrl("/topn"), TopNTree.class);
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

}
