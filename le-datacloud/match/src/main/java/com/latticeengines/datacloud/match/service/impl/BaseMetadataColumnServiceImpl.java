package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.camille.watchers.CamilleWatcher.AMMedataUpdate;
import static com.latticeengines.domain.exposed.camille.watchers.CamilleWatcher.AMRelease;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.camille.exposed.watchers.NodeWatcher;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.domain.exposed.datacloud.manage.MetadataColumn;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

public abstract class BaseMetadataColumnServiceImpl<E extends MetadataColumn> implements MetadataColumnService<E> {

    private static final Log log = LogFactory.getLog(BaseMetadataColumnServiceImpl.class);

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
    public void updateMetadataColumns(String dataCloudVersion, List<ColumnMetadata> columnMetadatas) {
        List<E> metadataColumns = new ArrayList<>();
        for (ColumnMetadata columnMetadata : columnMetadatas) {
            metadataColumns.add(updateSavedMetadataColumn(dataCloudVersion, columnMetadata));
        }
        getMetadataColumnEntityMgr().updateMetadataColumns(dataCloudVersion, metadataColumns);
        NodeWatcher.updateWatchedData(AMMedataUpdate.name(), NamingUtils.getFormatedDate());
        // pause 2 sec for watchers to update
        try {
            Thread.sleep(2000L);
        } catch (InterruptedException e) {
            log.warn(e);
        }
    }

    @Override
    @MatchStep
    public List<E> getMetadataColumns(List<String> columnIds, String dataCloudVersion) {
        List<E> toReturn = new ArrayList<>();
        ConcurrentMap<String, ConcurrentMap<String, E>> whiteColumnCaches = getWhiteColumnCache();
        if (!whiteColumnCaches.containsKey(dataCloudVersion)) {
            log.warn("Missed metadata cache for version " + dataCloudVersion);
            List<E> columns = getMetadataColumnEntityMgr().findAll(dataCloudVersion);
            Map<String, E> columnMap = new HashMap<>();
            for (E column : columns) {
                columnMap.put(column.getColumnId(), column);
            }
            for (String columnId : columnIds) {
                if (columnMap.containsKey(columnId)) {
                    toReturn.add(columnMap.get(columnId));
                } else {
                    toReturn.add(null);
                }
            }
        } else {
            ConcurrentSkipListSet<String> blackColumnCache = getBlackColumnCache().get(dataCloudVersion);
            for (String columnId : columnIds) {
                if (blackColumnCache != null && blackColumnCache.contains(columnId)) {
                    toReturn.add(null);
                } else if (!whiteColumnCaches.get(dataCloudVersion).containsKey(columnId)) {
                    toReturn.add(loadColumnMetadataById(columnId, dataCloudVersion));
                } else {
                    toReturn.add(whiteColumnCaches.get(dataCloudVersion).get(columnId));
                }
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
            ConcurrentSkipListSet<String> blackColumnCache = getBlackColumnCache().get(dataCloudVersion);
            if (blackColumnCache == null) {
                blackColumnCache = new ConcurrentSkipListSet<>();
                getBlackColumnCache().put(dataCloudVersion, blackColumnCache);
            }
            blackColumnCache.add(columnId);
            return null;
        } else {
            ConcurrentMap<String, E> whiteColumnCache = getWhiteColumnCache().get(dataCloudVersion);
            if (whiteColumnCache == null) {
                whiteColumnCache = new ConcurrentHashMap<>();
                getWhiteColumnCache().put(dataCloudVersion, whiteColumnCache);
            }
            whiteColumnCache.put(columnId, column);
            return column;
        }
    }

    private void refreshCaches() {
        List<String> allVersions = getAllVersions();
        for (String dataCloudVersion : allVersions) {
            log.info("Start loading black and white column caches for version " + dataCloudVersion);
            List<E> columns = getMetadataColumnEntityMgr().findAll(dataCloudVersion);
            ConcurrentMap<String, E> whiteColumnCache = new ConcurrentHashMap<>();
            for (E column : columns) {
                whiteColumnCache.put(column.getColumnId(), column);
            }
            synchronized (getWhiteColumnCache()) {
                getWhiteColumnCache().put(dataCloudVersion, whiteColumnCache);
            }
            synchronized (getBlackColumnCache()) {
                getBlackColumnCache().clear();
            }
            log.info(
                    "Loaded " + whiteColumnCache.size() + " columns into white cache. version=" + dataCloudVersion);
        }
    }

    private void initWatcher() {
        // both AMRelease and AMMetadataUpdate can trigger refreshing caches
        NodeWatcher.registerWatcher(AMRelease.name());
        NodeWatcher.registerListener(AMRelease.name(), () -> {
            log.info("ZK watcher " + AMRelease.name() + " changed, updating white and black columns caches ...");
            refreshCaches();
        });
        NodeWatcher.registerWatcher(AMMedataUpdate.name());
        NodeWatcher.registerListener(AMMedataUpdate.name(), () -> {
            log.info("ZK watcher " + AMMedataUpdate.name() + " changed, updating white and black columns caches ...");
            refreshCaches();
        });
    }

    abstract protected MetadataColumnEntityMgr<E> getMetadataColumnEntityMgr();

    abstract protected ConcurrentMap<String, ConcurrentMap<String, E>> getWhiteColumnCache();

    abstract protected ConcurrentMap<String, ConcurrentSkipListSet<String>> getBlackColumnCache();

    abstract protected List<String> getAllVersions();

    abstract protected E updateSavedMetadataColumn(String dataCloudVersion, ColumnMetadata columnMetadata);

    abstract protected boolean refreshCacheNeeded(String version);

}
