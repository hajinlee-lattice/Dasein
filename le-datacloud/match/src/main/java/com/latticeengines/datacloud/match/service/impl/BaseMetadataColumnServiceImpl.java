package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.camille.watchers.CamilleWatcher.AMRelease;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.watchers.NodeWatcher;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.domain.exposed.datacloud.manage.MetadataColumn;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

public abstract class BaseMetadataColumnServiceImpl<E extends MetadataColumn> implements MetadataColumnService<E> {

    private static final Logger log = LoggerFactory.getLogger(BaseMetadataColumnServiceImpl.class);

    @PostConstruct
    private void postConstruct() {
        initWatcher();
    }

    @Override
    public List<E> findByColumnSelection(Predefined selectName, String dataCloudVersion) {
        return getMetadataColumnEntityMgr().findByTag(selectName.getName(), dataCloudVersion);
    }

    @Override
    public List<E> scan(String dataCloudVersion) {
        return getMetadataColumnEntityMgr().findAll(dataCloudVersion);
    }

    @Override
    @MatchStep
    public List<E> getMetadataColumns(List<String> columnIds, String dataCloudVersion) {
        List<E> toReturn = new ArrayList<>();
        ConcurrentMap<String, ConcurrentMap<String, E>> whiteColumnCaches = getWhiteColumnCache();

        if (!whiteColumnCaches.containsKey(dataCloudVersion)) {
            log.warn("Missed metadata cache for version " + dataCloudVersion + ", loading now.");
            refreshCacheForVersion(dataCloudVersion);
        }
        if (!whiteColumnCaches.containsKey(dataCloudVersion)) {
            throw new RuntimeException("Still cannot find " + dataCloudVersion + " in white column cache.");
        } else {
            ConcurrentSkipListSet<String> blackColumnCache = getBlackColumnCache().get(dataCloudVersion);
            for (String columnId : columnIds) {
                E column;
                if (blackColumnCache != null && blackColumnCache.contains(columnId)) {
                    column = null;
                } else if (!whiteColumnCaches.get(dataCloudVersion).containsKey(columnId)) {
                    column = loadColumnMetadataById(columnId, dataCloudVersion);
                } else {
                    column = whiteColumnCaches.get(dataCloudVersion).get(columnId);
                }
                if (column == null) {
                    log.warn("Cannot resolve metadata column for columnId=" + columnId);
                }
                toReturn.add(column);
            }
        }
        return toReturn;
    }

    @Override
    @MatchStep(threshold = 100L)
    public E getMetadataColumn(String columnId, String dataCloudVersion) {
        ConcurrentMap<String, ConcurrentMap<String, E>> whiteColumnCaches = getWhiteColumnCache();
        if (!whiteColumnCaches.containsKey(dataCloudVersion)
                || !whiteColumnCaches.get(dataCloudVersion).containsKey(columnId)) {
            return getMetadataColumnEntityMgr().findById(columnId, dataCloudVersion);
        }
        if (getBlackColumnCache().containsKey(dataCloudVersion)
                && getBlackColumnCache().get(dataCloudVersion).contains(columnId)) {
            return null;
        }
        if (!whiteColumnCaches.containsKey(dataCloudVersion)
                || !whiteColumnCaches.get(dataCloudVersion).containsKey(columnId)) {
            return loadColumnMetadataById(columnId, dataCloudVersion);
        } else {
            return whiteColumnCaches.get(dataCloudVersion).get(columnId);
        }
    }

    private E loadColumnMetadataById(String columnId, String dataCloudVersion) {
        synchronized (getWhiteColumnCache()) {
            return performThreadSafeColumnMetadataLoading(columnId, dataCloudVersion);
        }
    }

    private E performThreadSafeColumnMetadataLoading(String columnId, String dataCloudVersion) {
        E column;
        try {
            column = getMetadataColumnEntityMgr().findById(columnId, dataCloudVersion);
            log.debug("Loaded metadata from DB for columnId = " + columnId);
        } catch (Exception e) {
            log.error(String.format("Failed to retrieve column information for [%s]", columnId), e);
            return null;
        }
        if (column == null) {
            ConcurrentSkipListSet<String> blackColumnCache = getBlackColumnCache().computeIfAbsent(dataCloudVersion,
                    k -> new ConcurrentSkipListSet<>());
            blackColumnCache.add(columnId);
            return null;
        } else {
            ConcurrentMap<String, E> whiteColumnCache = getWhiteColumnCache().computeIfAbsent(dataCloudVersion,
                    k -> new ConcurrentHashMap<>());
            whiteColumnCache.put(columnId, column);
            return column;
        }
    }

    private void refreshCaches() {
        // refresh latest version for sure
        String latestVersion = getLatestVersion();
        refreshCacheForVersion(latestVersion);
        for (String cachedVersion : getWhiteColumnCache().keySet()) {
            if (!latestVersion.equals(cachedVersion)) {
                refreshCacheForVersion(cachedVersion);
            }
        }
        synchronized (getBlackColumnCache()) {
            getBlackColumnCache().clear();
        }
    }

    private void refreshCacheForVersion(String dataCloudVersion) {
       new Thread(() -> {
            log.info("Start loading black and white column caches for version " + dataCloudVersion);
            List<E> columns = getMetadataColumnEntityMgr().findAll(dataCloudVersion);
            log.info("Read " + columns.size() + " columns from DB for version " + dataCloudVersion);
            ConcurrentMap<String, E> whiteColumnCache = new ConcurrentHashMap<>();
            for (E column : columns) {
                whiteColumnCache.put(column.getColumnId(), column);
            }
            synchronized (getWhiteColumnCache()) {
                getWhiteColumnCache().put(dataCloudVersion, whiteColumnCache);
            }
            log.info("Loaded " + whiteColumnCache.size() + " columns into white cache. version=" + dataCloudVersion);
        }).run();
    }

    private void initWatcher() {
        NodeWatcher.registerWatcher(AMRelease.name());
        NodeWatcher.registerListener(AMRelease.name(), () -> {
            log.info("ZK watcher " + AMRelease.name() + " changed, updating white and black columns caches ...");
            refreshCaches();
        });
        refreshCaches();
    }

    abstract protected MetadataColumnEntityMgr<E> getMetadataColumnEntityMgr();

    abstract protected ConcurrentMap<String, ConcurrentMap<String, E>> getWhiteColumnCache();

    abstract protected ConcurrentMap<String, ConcurrentSkipListSet<String>> getBlackColumnCache();

    abstract protected String getLatestVersion();

}
