package com.latticeengines.propdata.match.service.impl;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.propdata.manage.MetadataColumn;
import com.latticeengines.domain.exposed.propdata.manage.Predefined;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.entitymanager.MetadataColumnEntityMgr;
import com.latticeengines.propdata.match.service.MetadataColumnService;
import com.newrelic.api.agent.Trace;

public abstract class BaseMetadataColumnServiceImpl<E extends MetadataColumn> implements MetadataColumnService<E> {

    private static final Log log = LogFactory.getLog(BaseMetadataColumnServiceImpl.class);

    @PostConstruct
    private void postConstruct() {
        loadCache();
    }

    @Override
    public List<E> findByColumnSelection(Predefined selectName) {
        return getMetadataColumnEntityMgr().findByTag(selectName.getName());
    }

    @Override
    @Trace
    @MatchStep(threshold = 500)
    public E getMetadataColumn(String columnId) {
        if (getBlackColumnCache().contains(columnId)) {
            return null;
        }

        if (!getWhiteColumnCache().containsKey(columnId)) {
            return loadColumnMetadataById(columnId);
        } else {
            return getWhiteColumnCache().get(columnId);
        }
    }

    @Trace
    private E loadColumnMetadataById(String columnId) {
        synchronized (getWhiteColumnCache()) {
            return performThreadSafeColumnMetadataLoading(columnId);
        }
    }

    @Trace
    private E performThreadSafeColumnMetadataLoading(String columnId) {
        E column;
        try {
            column = getMetadataColumnEntityMgr().findById(columnId);
            log.info("Loaded metadata from DB for columnId = " + columnId);
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

    @Override
    public void loadCache() {
        log.info("Start loading black and white column caches.");
        getBlackColumnCache().clear();
        getWhiteColumnCache().clear();
        List<E> columns = getMetadataColumnEntityMgr().findAll();
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
