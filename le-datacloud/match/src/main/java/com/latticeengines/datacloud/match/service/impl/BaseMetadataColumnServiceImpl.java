package com.latticeengines.datacloud.match.service.impl;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.domain.exposed.datacloud.manage.MetadataColumn;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.propdata.core.entitymgr.DataCloudVersionEntityMgr;
import com.newrelic.api.agent.Trace;

public abstract class BaseMetadataColumnServiceImpl<E extends MetadataColumn> implements MetadataColumnService<E> {

    private static final Log log = LogFactory.getLog(BaseMetadataColumnServiceImpl.class);

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    protected abstract boolean isLatestVersion(String dataCloudVersion);
    protected abstract String getLatestVersion();

    @PostConstruct
    private void postConstruct() {
        loadCache();
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
    @Trace
    @MatchStep(threshold = 500)
    public E getMetadataColumn(String columnId, String dataCloudVersion) {
        if (!isLatestVersion(dataCloudVersion)) {
            return getMetadataColumnEntityMgr().findById(columnId, dataCloudVersion);
        }
        if (getBlackColumnCache().contains(columnId)) {
            return null;
        }
        if (!getWhiteColumnCache().containsKey(columnId)) {
            return loadColumnMetadataById(columnId, dataCloudVersion);
        } else {
            return getWhiteColumnCache().get(columnId);
        }
    }

    @Trace
    private E loadColumnMetadataById(String columnId, String dataCloudVersion) {
        synchronized (getWhiteColumnCache()) {
            return performThreadSafeColumnMetadataLoading(columnId, dataCloudVersion);
        }
    }

    @Trace
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
            getBlackColumnCache().add(columnId);
            return null;
        } else {
            getWhiteColumnCache().put(columnId, column);
            return column;
        }
    }

    private void loadCache() {
        log.info("Start loading black and white column caches for version " + getLatestVersion());
        getBlackColumnCache().clear();
        getWhiteColumnCache().clear();
        List<E> columns = getMetadataColumnEntityMgr().findAll(getLatestVersion());
        synchronized (getWhiteColumnCache()) {
            for (E column : columns) {
                getWhiteColumnCache().put(column.getColumnId(), column);
            }
        }
        log.info("Loaded " + getWhiteColumnCache().size() + " columns into white cache.");
    }

    abstract protected MetadataColumnEntityMgr<E> getMetadataColumnEntityMgr();

    abstract protected ConcurrentMap<String, E> getWhiteColumnCache();

    abstract protected ConcurrentSkipListSet<String> getBlackColumnCache();

}
