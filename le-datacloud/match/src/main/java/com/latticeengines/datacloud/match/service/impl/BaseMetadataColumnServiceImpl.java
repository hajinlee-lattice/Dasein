package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.common.exposed.bean.BeanFactoryEnvironment.Environment.WebApp;
import static com.latticeengines.domain.exposed.camille.watchers.CamilleWatcher.AMRelease;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;

import com.latticeengines.camille.exposed.watchers.NodeWatcher;
import com.latticeengines.common.exposed.bean.BeanFactoryEnvironment;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.domain.exposed.datacloud.manage.MetadataColumn;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

import reactor.core.publisher.ParallelFlux;

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
    public Long count(String dataCloudVersion) {
        if (StringUtils.isBlank(dataCloudVersion)) {
            dataCloudVersion = getLatestVersion();
        }
        return getMetadataColumnEntityMgr().count(dataCloudVersion);
    }

    @Override
    public ParallelFlux<E> scan(String dataCloudVersion, Integer page, Integer size) {
        if (StringUtils.isBlank(dataCloudVersion)) {
            dataCloudVersion = getLatestVersion();
        }
        if (page == null && size == null) {
            return getMetadataColumnEntityMgr().findAll(dataCloudVersion);
        } else if (size == null || page == null) {
            throw new IllegalArgumentException("Must specify page and size when asking for a particular page");
        } else {
            return getMetadataColumnEntityMgr().findByPage(dataCloudVersion, page, size).parallel();
        }
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
            RetryTemplate retry = RetryUtils.getRetryTemplate(10);
            List<E> columns = retry.execute(ctx -> {
                if (ctx.getRetryCount() > 0) {
                    log.info("Attempt=" + (ctx.getRetryCount() + 1) //
                            + " get all columns for version " + dataCloudVersion);
                }
                return getMetadataColumnEntityMgr().findAll(dataCloudVersion).sequential().collectList().block();
            });
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
        BeanFactoryEnvironment.Environment currentEnv = BeanFactoryEnvironment.getEnvironment();
        if (WebApp.equals(currentEnv)) {
            NodeWatcher.registerWatcher(AMRelease.name());
            NodeWatcher.registerListener(AMRelease.name(), () -> {
                int waitInSec = (int) (Math.random() * 30);
                log.info(String.format(
                        "ZK watcher %s is changed. To avoid refresh congestion, wait for %d seconds before start.",
                        AMRelease.name(), waitInSec));
                Thread.sleep(waitInSec * 1000);
                log.info(String.format("For changed ZK watcher %s, updating white and black columns caches ...",
                        AMRelease.name()));
                refreshCaches();
            });
        }
        log.info("Initial loading - updating white and black columns caches ...");
        refreshCaches();
    }

    protected abstract MetadataColumnEntityMgr<E> getMetadataColumnEntityMgr();

    protected abstract ConcurrentMap<String, ConcurrentMap<String, E>> getWhiteColumnCache();

    protected abstract ConcurrentMap<String, ConcurrentSkipListSet<String>> getBlackColumnCache();

    protected abstract String getLatestVersion();

}
